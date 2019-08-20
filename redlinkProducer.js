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
    this.ett = config.producerETT;
    this.priority = config.priority;
    const node = this;
    const log = require('./log.js')(node).log;

    const nodeId = config.id.replace('.', '');
    const replyMsgTriggerName = 'replyMessage' + nodeId;


    alasql.fn[replyMsgTriggerName] = () => {
        //look up inMessages table to see if this producer actually sent the message
        const unreadRepliesSql = 'SELECT redlinkMsgId FROM replyMessages WHERE  storeName="' + node.producerStoreName + '" AND read=false AND redlinkProducerId="' + node.id + '"';
        const unreadReplies = alasql(unreadRepliesSql);
        sendMessage({
            debug: {
                storeName: node.producerStoreName,
                consumerName: node.producerConsumer,
                action: 'producerReplyRead',
                direction: 'inBound',
                'unreadReplies': unreadReplies
            }
        });
        if (unreadReplies && unreadReplies.length > 0) {
            let unreadMsgIdsStr = '(';
            unreadReplies.forEach(reply => {
                unreadMsgIdsStr += '"' + reply.redlinkMsgId + '",'
            });
            unreadMsgIdsStr = unreadMsgIdsStr.substring(0, unreadMsgIdsStr.length - 1) + ')';
            const msgsByThisProducerSql = 'SELECT * FROM inMessages WHERE redlinkMsgId IN ' + unreadMsgIdsStr + ' AND redlinkProducerId="' + node.id + '"';
            const msgsByThisProducer = alasql(msgsByThisProducerSql); //should be length one if reply got for message from this producer else zero
            if (msgsByThisProducer && msgsByThisProducer.length == 0) {
                //Strange problem, If I end up here, the message has already been processed and the reply needs to be removed, its usually caused by multiple high traffic triggers at the same time
                const deleteReplyMsg = 'DELETE from replyMessages WHERE storeName="' + node.producerStoreName + '" AND redlinkMsgId="' + unreadReplies[0].redlinkMsgId + '"';
                const deleteReply = alasql(deleteReplyMsg);
            } else {
                const daId = msgsByThisProducer[0].redlinkMsgId;
                const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkMsgId="' + daId + '"';
                const relevantReplies = alasql(relevanttReplySql); //should have just one result
                if (relevantReplies && relevantReplies.length > 0) {
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

    function getReply(daId, relevanttReplySql, relevantReplies) {
        const replyMessage = relevantReplies[0].replyMessage;
        const getPreservedSql = 'SELECT * from inMessages WHERE redlinkMsgId="' + daId + '"';
        const preserved = alasql(getPreservedSql)[0].preserved;
        const reply = {
            payload: base64Helper.decode(replyMessage),
            redlinkMsgId: daId,
            redlinkProducerId: relevantReplies[0].redlinkProducerId,
            preserved: base64Helper.decode(preserved)
        };
        const deleteReplyMsg = 'DELETE from replyMessages WHERE storeName="' + node.producerStoreName + '" AND redlinkMsgId="' + daId + '"';
        const deleteInMsg = 'DELETE from inMessages    WHERE storeName="' + node.producerStoreName + '" AND redlinkMsgId="' + daId + '"';
        const deleteReply = alasql(deleteReplyMsg);
        const deleteIn = alasql(deleteInMsg);
        return reply;
    }

    function sendMessage(msg) { //receive, notify, failure, debug
        const msgs = [];
        if (!node.sendOnly) { //receive, notify
            msgs.push(msg.receive);
            msgs.push(msg.notify);
        }
        msgs.push(msg.failure);
        msgs.push(msg.debug);
        node.send(msgs);
    }

    node.on("input", msg => {
        if (msg.topic === 'read' || msg.topic === 'producerReplyRead') {
            if (node.manualRead) {
                if (node.manualRead && msg.redlinkMsgId) { //redlinkMsgId specified
                    const daId = msg.redlinkMsgId;
                    const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkMsgId="' + daId + '"';
                    const relevantReplies = alasql(relevanttReplySql); //should have just one result
                    if (relevantReplies && relevantReplies.length > 0) {
                        const reply = getReply(daId, relevanttReplySql, relevantReplies);
                        sendMessage({receive: reply});
                    } else {
                        sendMessage({failure: {"error": "Store " + node.producerStoreName + " redlinkMsgId not found"}});
                    }
                } else //No redlinkMsgId given, so assume any message from this Producer
                if (node.manualRead) {
                    const daId = node.id;
                    const relevanttReplySql = 'SELECT * FROM replyMessages WHERE redlinkProducerId="' + daId + '"';
                    const relevantReplies = alasql(relevanttReplySql); //should have just one result
                    if (relevantReplies && relevantReplies.length > 0) {
                        const reply = getReply(relevantReplies[0].redlinkMsgId, relevanttReplySql, relevantReplies);
                        sendMessage({receive: reply});
                    } else {
                        sendMessage({failure: {"error": "Store " + node.producerStoreName + " redlinkProducerId does NOT have any messages"}});
                    }
                } else { //todo error
                }
            }
            return;
        } else {
            sendMessage({debug: 'msg.topic should be one of read or producerReplyRead'});
        }
        // Assume that this is an insert Producer message
        msg.redlinkMsgId = RED.util.generateId();
        const preserved = msg.preserved || '';
        delete msg.preserved;
        const encodedMessage = base64Helper.encode(msg);
        const encodedPreserved = base64Helper.encode(preserved);
        let service = node.producerConsumer;
        if (msg.topic && node.producerConsumer === '') {
            // Verify Service first if msg.topic, the service must exist, you cannot produce to a non existent service
            service = msg.topic;
            const storeName = node.producerStoreName;
            const meshName = storeName.substring(0, storeName.indexOf(':')); // Producers can only send to Consumers on the same mesh
            const consumer = alasql('SELECT * from (SELECT distinct globalServiceName from ( select * from globalStoreConsumers WHERE localStoreName LIKE "' + meshName + '%"' +
                ' union select * from localStoreConsumers  WHERE storeName      LIKE "' + meshName + '%") WHERE globalServiceName = "' + service + '") ');
            if (!consumer.length > 0) {
                service = '';
            }
        }
        if (service.length > 0) {
            //todo dont store message if > 50kB- read store location from settings.js file
            const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.redlinkMsgId + '","' + node.producerStoreName + '","' + service + '","' + encodedMessage +
                '",' + false + ',' + node.sendOnly + ',"' + node.id + '","' + encodedPreserved + '",' + Date.now() + ',' + node.priority + ')';
            alasql(msgInsertSql);
        } else {
            sendMessage({failure: {"error": "Store " + node.producerStoreName + " Does NOT know about this service"}});
        }
    });
};