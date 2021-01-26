import collections
import logging
from datetime import datetime
import math
from ipware import get_client_ip

from rest_framework.exceptions import ValidationError
from rest_framework.generics import CreateAPIView, ListAPIView
from rest_framework.response import Response
from rest_framework import status

from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections

from django.core.cache import cache
from django.conf import settings
from django.utils.translation import ugettext_lazy as _

from oscar.apps.order.models import OneClickOrder
from oscar.apps.partner.models import Partner
from api.mixins import BaseApiMixinView
from api.catalogue.documents import ProductDocument
from api.offices.serializers import PartnerSerializer
from search.serializers import ProductDocumentSerializer
from globlog.decorators import globlog
from external_api.tasks import send_one_click_orders_to_1c
from external_api.services.xport import XPort
from external_api.services.web1c import Web1c
from common.bridge import Bridge
from orders.models import Status, ClientStatus

from .serializers import OneClickOrderSerializer

logger = logging.getLogger('sentry')
django_logger = logging.getLogger('django')

client = connections.get_connection()
index = ProductDocument._doc_type.index
mapping = ProductDocument._doc_type.name

MAP_STATUS = {
    ('delivery', 1): {
        'name': _('Pickup'),
        'type': 'pickup',
    },
    ('delivery', 2): {
        'name': _('Courier Company'),
        'type': 'courier_company',
    },
    ('delivery', 3): {
        'name': _('Novaya Pochta'),
        'type': 'nova_pochta',
    },
}


@globlog
class OneClickOrderAPIView(BaseApiMixinView, CreateAPIView):
    model = OneClickOrder
    serializer_class = OneClickOrderSerializer
    queryset = OneClickOrder.objects.all()

    def get_or_update_contract_id(self, data):
        if self.request.session.get('is_authenticated', False):
            agreement_id = self.request.session.get('contract', {}).get('agreement_id')
            result = XPort(request=self.request)('UserInfo', params={
                'UserSessionID': self.request.session.get('session_id'),
            })
            if result and result['status'] == 'ok':
                prag_agreement_id = result['data'][0].get('AgreementID')
                if agreement_id != prag_agreement_id:
                    self.request.session['contract']['agreement_id'] = prag_agreement_id
                    return prag_agreement_id
            return agreement_id
        else:
            return data.get('agreement_id')

    def perform_create(self, serializer):
        data = serializer.validated_data
        key = '{}_{}'.format(data['product_id'], self.request.session.session_key)
        if cache.get(key):
            raise ValidationError(_('You have just created one-click order. Please, wait 1 minute and try again'))
        serializer.validated_data.update({'agreement_id': self.get_or_update_contract_id(data)})
        super().perform_create(serializer)
        if settings.WEB1C_DATA_EXCHANGE_CELERY_ENABLE:
            send_one_click_orders_to_1c.apply_async(args=(serializer.instance.id, ))
        else:
            send_one_click_orders_to_1c(serializer.instance.id)
        cache.set(key, 1, 60)


@globlog
class GetOrdersView(BaseApiMixinView, ListAPIView):
    xport_key_map = {
        'OrderSaleOffices': {
            'key': 'offices',
            'value': 'SalesOfficeID',
            'label': 'SalesOfficeName',
        },
        'OrderTerms': {
            'key': 'delivery',
            'value': 'TermsID',
            'label': 'TermsName',
        },
        'OrderStatuses': {
            'key': 'status',
            'value': 'OrderStateID',
            'label': 'OrderStateName',
        }
    }

    def _get_es_products_by_prag_ids(self, prag_ids, per_page):
        search = Search(
            using=client,
            index=index,
            doc_type=mapping
        )

        prag_filter = []
        for prag_id in prag_ids:
            prag_filter.append(Q({'term': {'prag_id': prag_id}}))

        search = search.query('bool', should=prag_filter).extra(size=per_page)
        es_response = search.execute()

        products = ProductDocumentSerializer(es_response, many=True).data

        products_dict = {}
        for product in products:
            products_dict.setdefault(product['prag_id'], {}).update(product)
        return products_dict

    def _xport_get_request(self, params):
        return XPort(request=self.request)('GetOrders', params={
            'IsArc': 0,
            'OrderDateFrom': params['date_from'],
            'OrderDateTo': params['date_to'],
            'ContractOrderStateID': '',
            'SalesOfficeID': params['office'],
            'TermsShipID': params['delivery'],
            'ContractOrderLineStateID': params['order_item_status'],
            'OrderNum': params['order_number'],
            'FindWareStr': params['brand_name'],
            'PageNo': params['page'],
            'ElemOnPage': params['per_page'],
            'Language': self.request.LANGUAGE_CODE,
            'UserSessionID': self.request.session.get('session_id'),
        })

    def _xport_get_orders(self, params):
        result = {
            'data': [],
            'extra_info': {}
        }
        xport_response = self._xport_get_request(params)
        if xport_response and xport_response['status'] == 'ok':
            result.update({
                'data': xport_response['data'],
                'extra_info': xport_response['extra_info'],
            })
        else:
            logger.error('XPort error order: %s' % result.get('msg', ''))
        return result

    def _web1c_get_orders_info_request(self, params):
        return Web1c(request=self.request)('OrdersInfo', params={
            'data': [{"orderline_id": x} for x in params.get('order_ids')]
        })

    def _web1c_get_orders_info(self, params):
        result = {}
        web1c_response = self._web1c_get_orders_info_request(params)
        if web1c_response and web1c_response['status'] == 'ok':
            result = {x.get('orderline_id'): x for x in web1c_response.get('data', [])}
        else:
            logger.error('Web1c error order info')
        return result

    def _get_order_statuses(self):
        statuses = {}
        qs = Status.objects.filter(is_active=True, prag_id__isnull=False).prefetch_related('client_status')
        for item in qs:
            client_status = item.client_status.first()
            if client_status:
                statuses.setdefault(item.prag_id, {}).update({
                    'name': client_status.title,
                    'position': client_status.position,
                    'image': client_status.image_list.url if client_status.image_list else None,
                    'color': client_status.color,
                    'show_expected': client_status.show_expected,
                    'show_comment': client_status.show_comment
                    if client_status.image_list else ''})
        return statuses

    def get_orders(self, xport_order_list, per_page):
        products_dict = self._get_es_products_by_prag_ids(
            list(map(lambda x: x.get('WareID'), xport_order_list)), per_page
        )
        order_ids = [x.get('NumOrderLine') for x in xport_order_list]
        orders_comment = self._web1c_get_orders_info({'order_ids': order_ids})
        order_statuses_dict = self._get_order_statuses()
        orders_dict = {}
        order_items_dict = {}
        for item in xport_order_list:
            item = item
            default_image = list(filter(
                lambda x: x['display_order'] == 1, products_dict.get(item['WareID'], {}).get('images', [])
            ))

            try:
                date = datetime.strptime(item['OrderDate'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M")
            except ValueError:
                date = datetime.strptime(item['OrderDate'], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M")

            orders_dict.setdefault(item['ContractOrderID'], {}).update({
                'id': item['ContractOrderID'],
                'number': item['OrderNum'],
                'office': item['SalesOfficeName'],
                'date': date,
            })
            if products_dict.get(item['WareID']):
                product_item = products_dict.get(item['WareID'])
            else:
                product_item = {
                    'title': item.get('WareName'),
                    'description': item.get('WareName'),
                    'trademark': {'description': item.get('TradeMarkName')},
                    'upc': item.get('WareNum')
                }
                logger.error('Orders empty product in es')

            order_items_dict.setdefault(item['ContractOrderID'], []).append({
                'id': item.get('NumOrderLine'),
                'product_id': product_item.get('id'),
                'image': default_image[0].get('original') if default_image else None,
                'title': product_item.get('title'),
                'description': product_item.get('description'),
                'description_en': product_item.get('description_en'),
                'description_ru': product_item.get('description_ru'),
                'description_uk': product_item.get('description_uk'),
                'trademark': product_item.get('trademark', {}).get('description'),
                'trademark_slug': product_item.get('trademark', {}).get('slug'),
                'upc': product_item.get('upc'),
                'ware_id': item['WareID'],
                'slug': product_item.get('slug'),
                'status': {
                    'name': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('name'),
                    'comment': orders_comment[item.get('NumOrderLine')]['comment'] if orders_comment.get(item.get('NumOrderLine')) else '',
                    'delivery': orders_comment[item.get('NumOrderLine')]['shipping'] if orders_comment.get(item.get('NumOrderLine')) else '',
                    'image': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('image'),
                    'position': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('position'),
                    'color': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('color'),
                    'show_expected': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('show_expected'),
                    'show_comment': order_statuses_dict.get(item['ContractOrderLineStateID'], {}).get('show_comment'),
                },
                'quantity': item['Quantity'],
                'price': int(item.get('CurrentClientPrice', 0)) * item.get('Quantity', 0),
                'price_item': int(item.get('CurrentClientPrice', 0)),
            })

        orders_list = []
        for key, value in orders_dict.items():
            value['list'] = order_items_dict.get(key)
            value['total'] = sum(map(lambda x: x['price'], order_items_dict.get(key)))
            orders_list.append(value)

        return orders_list

    def get_extra(self, xport_order_extra, xport_order_list, per_page):
        extra_data = {
            'offices': [],
            'delivery': [],
            'status': [],
            'total': None,
            'pages': None,
            'orders_total': None,
        }

        for key, value in xport_order_extra.items():
            for item in value:
                label_key = (self.xport_key_map[key]['key'], item[self.xport_key_map[key]['value']])
                extra_data[self.xport_key_map[key]['key']].append({
                    'value': item[self.xport_key_map[key]['value']],
                    'label': MAP_STATUS.get(label_key, {}).get('name', item[self.xport_key_map[key]['label']])
                })
        offices = Partner.objects.filter(prag_id__in=[x['value'] for x in extra_data.get('offices')])
        offices_dict = {x.prag_id: x.name for x in offices}
        offices_list = []
        for item in extra_data.get('offices'):
            offices_list.append({
                'value': item['value'],
                'label': offices_dict.get(item['value'], item['label'])
            })
        extra_data['offices'] = offices_list
        if xport_order_list:
            order_item = xport_order_list[0]
            orders_total = order_item.get('OrdersCountTotal', 0) + order_item.get('OrdersCountTotalArc', 0)
            extra_data['total'] = int(order_item.get('OrdersSumTotalNotСancel')) if order_item.get('OrdersSumTotalNotСancel', None) else 0
            extra_data['orders_total'] = orders_total
            extra_data['pages'] = math.ceil(orders_total / per_page)
        return extra_data

    def _update_xport_session_id(self):
        # No idea. Why we are using that stuff, especially about param HostName.
        xport_response = XPort(request=self.request)('SessionReg', params={
            'Mode': '0',
            'IPUser': get_client_ip(self.request),
            'UserSessionID': self.request.session.get('session_id', None),
            'HostName': 'localhost',
        })

        if xport_response and xport_response['status'] == 'ok':
            session_id = xport_response['data'][0]['UserSessionID']
            django_logger.error('XPort error order - empty order_list results. New session: %s' % str(session_id))
            Bridge.update_session_id({'session_id': session_id}, self.request)
            return True
        return False

    def list(self, request, *args, **kwargs):
        if not self.request.session.get('is_authenticated', False):
            return Response({}, status.HTTP_204_NO_CONTENT)

        date_from = request.GET.get('date_from')
        date_to = request.GET.get('date_to')
        if date_from:
            date_from = int(
                datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%S.%fZ").replace(hour=3, minute=0).timestamp()
            )
        if date_to:
            date_to = int(
                datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%S.%fZ").replace(hour=23, minute=59).timestamp()
            )
        per_page = int(request.COOKIES.get('orders_page_size', settings.DEFAULT_ORDERS_PAGE_SIZE))
        page = int(request.GET.get('page', 1))
        params = {
            'brand_name': request.GET.get('brand_name'),
            'date_from': date_from,
            'date_to': date_to,
            'delivery': request.GET.get('delivery'),
            'office': request.GET.get('office'),
            'order_number': request.GET.get('order_number'),
            'order_item_status': request.GET.get('order_item_status'),
            'page': page,
            'per_page': per_page,
        }

        xport_response = self._xport_get_orders(params)
        if not xport_response['data']:
            if self._update_xport_session_id():
                xport_response = self._xport_get_orders(params)

        extra_info = self.get_extra(xport_response['extra_info'], xport_response['data'], per_page)
        result = {
            'orders': self.get_orders(xport_response['data'], per_page),
            'offices': extra_info.get('offices'),
            'delivery': extra_info.get('delivery'),
            'status': extra_info.get('status'),
            'total': extra_info.get('total'),
            'pages': extra_info.get('pages'),
            'orders_total': extra_info.get('orders_total'),
            'per_page': per_page,
            'current_page': page,
            'previous': page - 1,
            'next': page + 1,
        }
        return Response(result)


SHIPPING_DICT = {
    'Самовывоз': 'office',
    'ДоКлиента': 'client',
    'СиламиПеревозчика': 'np',
}


@globlog
class GetOrdersStatesView(BaseApiMixinView, ListAPIView):

    def _web1c_get_orders_states_request(self, params):
        return Web1c(request=self.request)('OrdersStates', params={
            'data': [{"orderline_id": x} for x in params.get('order_ids')]
        })

    def _web1c_get_orders_states(self, params):
        result = {}
        web1c_response = self._web1c_get_orders_states_request(params)
        if web1c_response and web1c_response['status'] == 'ok':
            result = {x.get('orderline_id'): x for x in web1c_response.get('data', [])}
        else:
            logger.error('Web1c error order states')
        return result

    def get_orders_status(self):
        result = {}
        for item in Status.objects.filter(is_active=True).prefetch_related('client_status'):
            client_status = item.client_status.filter(is_active=True).first()
            if client_status:
                result[item.erp_id] = {
                    'id': client_status.id,
                    'title': client_status.title,
                    'position': client_status.position,
                    'color': client_status.color,
                    'image_list': client_status.image_list.url if client_status.image_list else None,
                    'image': client_status.image.url if client_status.image else None,
                }
            else:
                logger.error('Orders error: can not find client status for status id = %s' % str(item.id))
        return result

    def get_office(self, code):
        result = None
        if code:
            office_object = Partner.objects.filter(code=code).first()
            if office_object:
                result = PartnerSerializer(office_object).data
        return result

    def list(self, request, *args, **kwargs):
        order_id = kwargs.get('order_id')
        if not self.request.session.get('is_authenticated', False) or not order_id:
            return Response({}, status.HTTP_204_NO_CONTENT)

        orders_history = self._web1c_get_orders_states({'order_ids': [order_id]})
        orders_status = self.get_orders_status()
        web1c_order = orders_history.get(order_id, {})
        if not web1c_order:
            return Response({}, status.HTTP_204_NO_CONTENT)

        office = self.get_office(web1c_order.get('sales_office'))
        result = {
            'shipping': web1c_order.get('shipping'),
            'shipping_addr': web1c_order.get('shipping_addr'),
            'delivery_num': web1c_order.get('delivery_num'),
            'delivery_status': web1c_order.get('delivery_status'),
            'canceled': web1c_order.get('canceled'),
            'delivery_date': datetime.strptime(web1c_order.get('delivery_date'), "%Y-%m-%dT%H:%M:%S").strftime(
                "%d.%m.%Y") if web1c_order.get('delivery_date') else None,
            'office': office,
            'states': {},
            'percent': 0,
            'total_active': 0,
            'current': None,
            'order_item_id': order_id,
            'color': None,
            'image': None,
            'type': SHIPPING_DICT.get(web1c_order.get('shipping_type'), 'office'),
        }
        states = collections.OrderedDict()
        web1c_states = web1c_order.get('states', [])
        web1c_states.reverse()
        for item in web1c_states:
            order_item = orders_status.get(item.get('id'), {})
            item_date = None
            if not orders_status.get(item.get('id')):
                continue
            if item.get('date'):
                if order_item.get('position') == 'last':
                    item_date = datetime.strptime(
                        item.get('date'), "%Y-%m-%dT%H:%M:%S").strftime("%d.%m.%Y")
                else:
                    item_date = datetime.strptime(
                        item.get('date'), "%Y-%m-%dT%H:%M:%S").strftime("%d.%m.%Y %H:%M")
            state = {
                'id': order_item.get('id'),
                'title': order_item.get('title'),
                'image': order_item.get('image'),
                'image_list': order_item.get('image_list'),
                'color': order_item.get('color'),
                'date': item_date,
                'active': True if item.get('date') else False,
                'position': order_item.get('position'),
            }
            states[order_item.get('id')] = state
        pickup = ClientStatus.objects.filter(position='pickup').first()
        if pickup and states.get(pickup.id):
            pickup = None
        last = ClientStatus.objects.filter(position='last').first()
        if last and states.get(last.id):
            last = None
        states_list = list(states.values())
        if pickup:
            states_list.insert(0, {
                'id': pickup.id,
                'title': pickup.title,
                'image': pickup.image.url if pickup.image else None,
                'image_list': pickup.image_list.url if pickup.image_list else None,
                'color': pickup.color,
                'date': None,
                'active': False,
                'position': pickup.position,
            })
        if last:
            states_list.insert(0, {
                'id': last.id,
                'title': last.title,
                'image': last.image.url if last.image else None,
                'image_list': last.image_list.url if last.image_list else None,
                'color': last.color,
                'date': None,
                'active': False,
                'position': last.position,
            })
        states_list_reverse = states_list.copy()
        states_list_reverse.reverse()
        for item in states_list:
            if item.get('title') and item.get('active'):
                result['total_active'] += 1
                result['current'] = item.get('id') if not result['current'] else result['current']
        for item in states_list_reverse:
            if item.get('title') and item.get('active'):
                result['color'] = item.get('color')
                result['image'] = item.get('image_list')
        result['states'] = states_list
        result['percent'] = (result['total_active'] - 1) * math.floor(100 / (len(result['states']) - 1))
        result['percent'] += result['total_active'] - 2 if len(states_list) > 6 else 0
        if result['percent'] > 95:
            result['percent'] = 100
        return Response(result)