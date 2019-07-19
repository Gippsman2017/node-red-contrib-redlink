const alasql = require('alasql');

const base64Helper = require('./base64-helper.js');


let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkProducer = function (config) {
    RED.nodes.createNode(this, config);
    this.producerStoreName = config.producerStoreName;
    this.producerConsumer = config.producerConsumer;
    this.sendOnly = config.sendOnly;
    const node = this;
    const log = require('./log.js')(node).log;

    const nodeId = config.id.replace('.', '');
    const replyMsgTriggerName = 'replyMessage' + nodeId;
    alasql.fn[replyMsgTriggerName] = () => {
        //look up inMessages table to see if this producer actually sent the message
        const unreadRepliesSql = 'SELECT redlinkMsgId FROM replyMessages WHERE read=false';
        const unreadReplies = alasql(unreadRepliesSql);
        log('in trigger unread replies:', unreadReplies);
        if (unreadReplies && unreadReplies.length > 0) {
            let unreadMsgIdsStr =  '(';
            unreadReplies.forEach(reply=>{
                unreadMsgIdsStr += '"'+reply.redlinkMsgId+'",'
            });
            unreadMsgIdsStr = unreadMsgIdsStr.substring(0, unreadMsgIdsStr.length - 1) +')';
            log('unreadMsgIdsStr here 2 :', unreadMsgIdsStr );
            const msgsByThisProducerSql = 'SELECT * FROM inMessages WHERE redlinkMsgId IN '+unreadMsgIdsStr+' AND producerId="'+node.id+'"';
            log('msgsByThisProducerSql:', msgsByThisProducerSql);
            const msgsByThisProducer = alasql(msgsByThisProducerSql); //should be length one if reply got for message from this producer else zero
            log('msgsByThisProducer :', msgsByThisProducer);
            if(msgsByThisProducer && msgsByThisProducer.length>0 ){
                const daId = msgsByThisProducer[0].redlinkMsgId;
                const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkMsgId="'+ daId +'"';
                const relevantReplies = alasql(relevanttReplySql); //should have just one result
                if(relevantReplies && relevantReplies.length >0){
                    const updateReplySql = 'UPDATE replyMessages SET READ=true WHERE redlinkMsgId="' + daId + '"';
                    alasql(updateReplySql);
                    log('replies after updating:', alasql(relevanttReplySql));
                    const replyMessage = relevantReplies[0].replyMessage;
                    const reply = {
                        payload: base64Helper.decode(replyMessage),
                        redlinkMsgId : daId,
                        topic: relevantReplies[0].topic
                    };
                    log('in producer going to send out reply as:', JSON.stringify(reply, null, 2));
                    node.send(reply);
                }
            }
        }
    };
    const createReplyMsgTriggerSql = 'CREATE TRIGGER ' + replyMsgTriggerName + ' AFTER INSERT ON replyMessages CALL ' + replyMsgTriggerName + '()';
    alasql(createReplyMsgTriggerSql);

    node.on("input", msg => {
        if (!msg.redlinkMsgId) {
          msg.redlinkMsgId = RED.util.generateId();
          };
        const stringify = JSON.stringify(msg);
        const encodedMessage = base64Helper.encode(msg);
        log('the input message is:', stringify);
        const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.redlinkMsgId + '","' + this.producerStoreName +
            '","' + this.producerConsumer + '","' + encodedMessage + '",' + false + ',' + node.sendOnly + ',"' + node.id + '")';
        log('in the producer going to execute sql to insert into inmesasges: ', msgInsertSql);
        alasql(msgInsertSql);
    });
};