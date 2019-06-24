const alasql = require('alasql');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkProducer = function (config) {
    RED.nodes.createNode(this, config);
    this.producerStoreName = config.producerStoreName;
    this.producerConsumer  = config.producerConsumer;
    var node = this;
    node.on("input", msg => {
        msg.msgid = RED.util.generateId();
        const stringify = JSON.stringify(msg);
        const encodedMessage = base64Helper.encode(msg);
        console.log('the input message is:', stringify);
        const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.msgid + '","' + this.producerStoreName + '","' + this.producerConsumer + '","' + encodedMessage + '")';
        console.log('in the consumer going to execute sql to insert into inmesasges: ', msgInsertSql);
        alasql(msgInsertSql);
        const allRows = alasql('select * from inMessages');
        console.log('after inserting input message the inMessages table is:', allRows[0]);
    })
};