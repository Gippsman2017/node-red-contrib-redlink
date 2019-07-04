module.exports = function(node) {
    this.node = node;

    this.log = function() {
        let i = 0;
        let str = '';
        for (; i < arguments.length; i++) {
            str += ' ' + JSON.stringify(arguments[i], null, 2) + ' ';
        }
        if(node){
            node.trace(str);
        }else{
            console.log('node not initialised in log- reverting to console.log', str);
        }
    };
    return this;
};

