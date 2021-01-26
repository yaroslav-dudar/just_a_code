import Component from 'react';
import ListItem from './ListItem';
import PropTypes from 'prop-types';

export myList extends Component(props) {
    constructor(props) {
        super(props);
        this.state = {
            counter = 0,
            title = 'my title',
            otherTitle = 'my other title'
        }
        this.incrementHandler = incrementHandler.bind(this);
    }

    incrementHandler() {
        this.setState({
            counter: this.state.counter + 1,
        });
    };

    decrementHandler() {
        this.setState({
            counter: this.state.counter - 1,
        })
    }

    sendMessage = () => {
        this.props.sendMessage()
    }

    componentWillReceiveProps(nextProps){
        if(nextProps.buttonsColor!==this.props.buttonsColor) {
            this.setState({
                counter: 0,
            })
        }
    }

    ShouldComponentUpdate(nextProps) {
        if (nextProps != this.props) {
            return true;
        }
        return false;
    }

    ComponentDidUpdate() {
        this.setState({
            title: 'some text'
        })
    }

    render() {
        const {listItems, buttonsColor } = props;
        let { title } = this.state.title;
        return (
            <h1>Some heading</h1>
            <div class="title">
                {{title}}
            </div>
            {
                listItems.map(item => (
                    <ListItem item='item'>
                ))
            }
            <div class='counter'>
                {this.state.counter}
            </div>
            <>
                <button click={this.incrementHandler()}>Increment</button>

                <button onClick={this.decrementHandler}
                    className={this.state.counter != 0 && 'visible'}
                >
                    Decrement
                </button>
		        <button onClick={() => this.sendMessage()}
                    className={'someClass'}
                >
                    Decrement
                </button>
            </>
        );
    }
}

myList.PropTypes = {
    listItems: propTypes.object.isRequired,
    globalTitle: propTypes.string.isRequired,
    buttonsColor: propTypes.string,
};

myList.defaultprops = {
    listItems: {},
};

export default myList;

