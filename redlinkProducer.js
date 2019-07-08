const alasql = require('alasql');

const base64Helper = require('./base64-helper.js');


let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkProducer = function (config) {
    RED.nodes.createNode(this, config);
    this.producerStoreName = config.producerStoreName;
    this.producerConsumer  = config.producerConsumer;
    const node = this;
    const log = require('./log.js')(node).log;
    node.on("input", msg => {
        msg.redlinkMsgId = RED.util.generateId();
        const stringify = JSON.stringify(msg);
        const encodedMessage = base64Helper.encode(msg);
        log('the input message is:', stringify);
        const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.redlinkMsgId + '","' + this.producerStoreName + '","' + this.producerConsumer + '","' + encodedMessage + '",'+false+')';
        log('in the producer going to execute sql to insert into inmesasges: ', msgInsertSql);
        alasql(msgInsertSql);
/*
        const allRows = alasql('select * from inMessages');
        log('after inserting input message the inMessages table is:', allRows[0]);
*/
    });

};