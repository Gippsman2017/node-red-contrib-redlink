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
    this.sendOnly = config.sendOnly;
    const node = this;
    const log  = require('./log.js')(node).log;

    const nodeId = config.id.replace('.', '');
    const replyMsgTriggerName = 'replyMessage' + nodeId;
    alasql.fn[replyMsgTriggerName] = () => {
        //look up inMessages table to see if this producer actually sent the message
        const unreadRepliesSql = 'SELECT redlinkMsgId FROM replyMessages WHERE read=false';
        const unreadReplies    = alasql(unreadRepliesSql);

        log('in trigger unread replies:', unreadReplies);
        node.send([null,null,{storeName: node.producerStoreName,consumerName:node.producerConsumer,action:'producerReplyRead',direction:'inBound'}]);

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
//                    const updateReplySql = 'DELETE from replyMessages WHERE redlinkMsgId="' + daId + '"';
                    const updateReplySql = 'UPDATE replyMessages SET READ=true WHERE redlinkMsgId="' + daId + '"';
                    alasql(updateReplySql);
                    log('replies after updating:', alasql(relevanttReplySql));
                    const replyMessage = relevantReplies[0].replyMessage;
                    const reply = {
                        payload: base64Helper.decode(replyMessage),
                        redlinkMsgId : daId,
                        topic: relevantReplies[0].topic,
                        preserved :  base64Helper.decode(msgsByThisProducer[0].preserved)
                    };
                log('in producer going to send out reply as:', JSON.stringify(reply, null, 2));

                node.send([reply,reply,{storeName: node.producerStoreName,consumerName:node.producerConsumer,action:'producerReplyRead',direction:'outBound',reply}]);

                const deleteReplyMsg  = 'DELETE from replyMessages WHERE storeName="'+node.producerStoreName+'" AND redlinkMsgId="' +  daId + '"';
                const deleteInMsg     = 'DELETE from inMessages    WHERE storeName="'+node.producerStoreName+'" AND redlinkMsgId="' +  daId + '"';
                const deleteNotifyMsg = 'DELETE from notify        WHERE redlinkMsgId="' +  daId + '"';

                log('Producer Delete Reply  = ', alasql(deleteReplyMsg));
                log('Producer Delete inMsg  = ', alasql(deleteInMsg));
                log('Delete Consumer Notify = ', alasql(deleteNotifyMsg));
                }
            }
        }
    };
    const createReplyMsgTriggerSql = 'CREATE TRIGGER ' + replyMsgTriggerName + ' AFTER INSERT ON replyMessages CALL ' + replyMsgTriggerName + '()';
    alasql(createReplyMsgTriggerSql);

    node.on("input", msg => {
        node.send([null,null,{msg:msg,payload:msg.payload,enpayload:base64Helper.encode(msg.payload),decpayload:base64Helper.decode(base64Helper.encode(msg.payload))}]);
// We will assume for now that every message is unique
//        if (!msg.redlinkMsgId) {
        msg.redlinkMsgId = RED.util.generateId();
//          };
        const preserved = msg.preserved;
        delete msg.preserved;
        const encodedMessage   = base64Helper.encode(msg);
        const encodedPreserved = base64Helper.encode(preserved);
//        log('the input message is:', stringify);
        const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.redlinkMsgId + '","' + node.producerStoreName + '","' + node.producerConsumer + '","' + encodedMessage + 
                                                          '",'  + false            + ','   + node.sendOnly          + ',"' + node.id                + '","' + encodedPreserved + '")';
        log('in the producer going to execute sql to insert into inmesasges: ', msgInsertSql);
        alasql(msgInsertSql);
    });
};