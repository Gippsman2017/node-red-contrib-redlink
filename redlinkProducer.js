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
    this.debug = config.showDebug;
    this.manualRead = config.manualRead;
    const node = this;
    const log = require('./log.js')(node).log;

    const nodeId = config.id.replace('.', '');
    const replyMsgTriggerName = 'replyMessage' + nodeId;


    alasql.fn[replyMsgTriggerName] = () => {
        //look up inMessages table to see if this producer actually sent the message
        const unreadRepliesSql = 'SELECT redlinkMsgId FROM replyMessages WHERE read=false';
        const unreadReplies = alasql(unreadRepliesSql);
        log('in trigger unread replies:', unreadReplies);
        sendMessage({debug: {storeName: node.producerStoreName,consumerName:node.producerConsumer,action:'producerReplyRead',direction:'inBound'}});
        if (unreadReplies && unreadReplies.length > 0) {
            let unreadMsgIdsStr = '(';
            unreadReplies.forEach(reply => { unreadMsgIdsStr += '"' + reply.redlinkMsgId + '",' });
            unreadMsgIdsStr = unreadMsgIdsStr.substring(0, unreadMsgIdsStr.length - 1) + ')';
            
            const msgsByThisProducerSql = 'SELECT * FROM inMessages WHERE redlinkMsgId IN ' + unreadMsgIdsStr + ' AND producerId="' + node.id + '"';
            const msgsByThisProducer = alasql(msgsByThisProducerSql); //should be length one if reply got for message from this producer else zero
            if (msgsByThisProducer && msgsByThisProducer.length > 0) {
                const daId = msgsByThisProducer[0].redlinkMsgId;
                const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkMsgId="' + daId + '"';
                const relevantReplies = alasql(relevanttReplySql); //should have just one result
                if(relevantReplies && relevantReplies.length >0){
                    const notifyMessage = { //todo store more info in replyMesssages- store, etc and populate in notify msg
                        redlinkMsgId: daId,
                        notifyType: 'producerReplyNotification',
                    };
                    if (node.manualRead) {
                        sendMessage({notify: notifyMessage})
                    } else {
                        const reply = getReply(daId, relevanttReplySql, relevantReplies, msgsByThisProducer[0].preserved);
                        sendMessage({receive: reply}, {notify: notifyMessage});
                    }
                }
            }
        }
    };
    const createReplyMsgTriggerSql = 'CREATE TRIGGER ' + replyMsgTriggerName + ' AFTER INSERT ON replyMessages CALL ' + replyMsgTriggerName + '()';
    alasql(createReplyMsgTriggerSql);

    function getReply(daId, relevanttReplySql, relevantReplies, preserved) {
        const updateReplySql = 'UPDATE replyMessages SET READ=true WHERE redlinkMsgId="' + daId + '"';
        alasql(updateReplySql);
        const replyMessage = relevantReplies[0].replyMessage;
        const reply = {
            payload: base64Helper.decode(replyMessage),
            redlinkMsgId: daId,
            topic: relevantReplies[0].topic,
            preserved :  base64Helper.decode(preserved)
        };
        log('in producer going to send out reply as:', JSON.stringify(reply, null, 2));
        const deleteReplyMsg  = 'DELETE from replyMessages WHERE storeName="'+node.producerStoreName+'" AND redlinkMsgId="' +  daId + '"';
        const deleteInMsg     = 'DELETE from inMessages    WHERE storeName="'+node.producerStoreName+'" AND redlinkMsgId="' +  daId + '"';
        const deleteNotifyMsg = 'DELETE from notify        WHERE redlinkMsgId="' +  daId + '"';
        const deleteReply = alasql(deleteReplyMsg);
        const deleteIn = alasql(deleteInMsg);
        const deleteNotify = alasql(deleteNotifyMsg);
        return reply;
    }

    function sendMessage(msg) { //receive, notify, failure, debug
        const msgs = [];
        if (!node.sendOnly) { //receive, notify
            if (msg.receive) { msgs.push(msg.receive); } 
                        else { msgs.push(null); }
            if (msg.notify)  { msgs.push(msg.notify); } 
                        else { msgs.push(null); }
        }
        if (msg.failure) { msgs.push(msg.failure); } 
                    else { msgs.push(null); }
        if (node.debug)  {
            if (msg.debug)  {  msgs.push(msg.debug); } 
                       else {  msgs.push(null); }
        }
        node.send(msgs);
    }

    node.on("input", msg => {
        if (msg.cmd === 'read') {
            if (node.manualRead && msg.redlinkMsgId) {
                const daId = msg.redlinkMsgId;
                const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkMsgId="' + daId + '"';
                const relevantReplies = alasql(relevanttReplySql); //should have just one result
                if (relevantReplies && relevantReplies.length > 0) {
                    const reply = getReply(daId, relevanttReplySql, relevantReplies);
                    sendMessage({receive: reply});
                }
            } else { //todo error

            }
            return;
        }
        msg.redlinkMsgId = RED.util.generateId();
        const preserved = msg.preserved || '';
        delete msg.preserved;
        const encodedMessage   = base64Helper.encode(msg);
        const encodedPreserved = base64Helper.encode(preserved);
        const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.redlinkMsgId + '","' + node.producerStoreName + '","' + node.producerConsumer + '","' + encodedMessage +
                                                          '",'  + false            + ','   + node.sendOnly          + ',"' + node.id                + '","' + encodedPreserved + '")';
        alasql(msgInsertSql);
    });
};