const alasql = require('alasql');
const fs = require('fs-extra');

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
    const largeMessagesDirectory = require('./redlinkSettings.js')(RED, node.producerStoreName).largeMessagesDirectory;
    const largeMessageThreshold = require('./redlinkSettings.js')(RED, node.producerStoreName).largeMessageThreshold;
    let errorMsg = null;
    try {
        fs.ensureDirSync(largeMessagesDirectory);
    } catch (e) {
        errorMsg = 'Unable to create directory to store large messages. ' + e.toString();
        sendMessage({failure: errorMsg, debug: errorMsg});
    }

    console.log('largeMessagesDirectory:', largeMessagesDirectory);
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
            if (msgsByThisProducer && msgsByThisProducer.length === 0) {
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

    function getReplyMessage(relevantReply) {
        if(relevantReply && relevantReply.isLargeMessage){
            //read from disk and return;
            const path = largeMessagesDirectory + relevantReply.redlinkMsgId +'/reply.txt';
            //read msg from path
            return fs.readFileSync(path, 'utf-8');
        }
        return relevantReply.replyMessage;
    }

    function getReply(daId, relevanttReplySql, relevantReplies) {
        const replyMessage = getReplyMessage(relevantReplies[0]);
        const origMessageSql = 'SELECT * from inMessages WHERE redlinkMsgId="' + daId + '"';
        const origMessage = alasql(origMessageSql)[0];
        let preserved;
        if(origMessage.isLargeMessage){
            //read preserved from file
            const path = largeMessagesDirectory + daId + '/preserved.txt';
            preserved = fs.readFileSync(path, 'utf-8');
        }else{
            preserved = origMessage.preserved;
        }
        const reply = {
            payload: base64Helper.decode(replyMessage),
            redlinkMsgId: daId,
            redlinkProducerId: relevantReplies[0].redlinkProducerId,
            preserved: base64Helper.decode(preserved)
        };

        const deleteReplyMsg = 'DELETE from replyMessages WHERE storeName="' + node.producerStoreName + '" AND redlinkMsgId="' + daId + '"';
        const deleteInMsg = 'DELETE from inMessages    WHERE storeName="' + node.producerStoreName + '" AND redlinkMsgId="' + daId + '"';
        //delete directory from disk if large inMessage/ replyMessage
        if(origMessage.isLargeMessage || relevantReplies[0].isLargeMessage){
            const path = largeMessagesDirectory + daId + '/';
            fs.removeSync(path);
        }
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

    function isLargeMessage(encodedMessage, encodedPreserved) {
        return encodedMessage.length + encodedPreserved.length > largeMessageThreshold;
    }

    function insertNewMessage(redlinkMsgId, service, encodedMessage, encodedPreserved, isLargeMessage) {
        if (isLargeMessage) {
            const path = largeMessagesDirectory + redlinkMsgId + '/';
            try {
                fs.ensureDirSync(largeMessagesDirectory);
            } catch (e) {
                errorMsg = 'Unable to create directory to store large messages. ' + e.toString();
                sendMessage({failure: errorMsg, debug: errorMsg});
                return;
            }
            try {
                fs.outputFileSync(path + 'message.txt', encodedMessage);
                fs.outputFileSync(path + 'preserved.txt', encodedPreserved);
            } catch (e) {
                errorMsg = 'Unable to write large message to file ' + e.toString();
                sendMessage({failure: errorMsg, debug: errorMsg});
                return;
            }
            const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + redlinkMsgId + '","' + node.producerStoreName + '","' + service + '",""' +
                ',' + false + ',' + node.sendOnly + ',"' + node.id + '","",' + Date.now() + ',' + node.priority + ',' + true + ')';
            //redlinkMsgId STRING, storeName STRING, serviceName STRING, message STRING, read BOOLEAN, sendOnly BOOLEAN, redlinkProducerId STRING,preserved STRING, timestamp BIGINT, priority INT, isLargeMessage BOOLEAN, path STRING
            alasql(msgInsertSql);
        } else {
            const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + redlinkMsgId + '","' + node.producerStoreName + '","' + service + '","' + encodedMessage +
                '",' + false + ',' + node.sendOnly + ',"' + node.id + '","' + encodedPreserved + '",' + Date.now() + ',' + node.priority + ',' + false + ')';
            //redlinkMsgId STRING, storeName STRING, serviceName STRING, message STRING, read BOOLEAN, sendOnly BOOLEAN, redlinkProducerId STRING,preserved STRING, timestamp BIGINT, priority INT,
            // isLargeMessage BOOLEAN, path STRING
            alasql(msgInsertSql);
        }
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
            const largeMessage = isLargeMessage(encodedMessage, encodedPreserved);
            if (largeMessage) {
                if (errorMsg) {
                    sendMessage({failure: errorMsg, debug: errorMsg});
                    return;
                }
                insertNewMessage(msg.redlinkMsgId, service, encodedMessage, encodedPreserved, true);
                //redlinkMsgId STRING, storeName STRING, serviceName STRING, message STRING, read BOOLEAN, sendOnly BOOLEAN, redlinkProducerId STRING,preserved STRING, timestamp BIGINT, priority INT
            } else {
                insertNewMessage(msg.redlinkMsgId, service, encodedMessage, encodedPreserved, false);
            }
        } else {
            sendMessage({failure: {"error": "Store " + node.producerStoreName + " Does NOT know about this service"}});
        }
    });
};