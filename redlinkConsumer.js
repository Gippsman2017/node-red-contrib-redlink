const alasql = require('alasql');
const RateLimiter = require('limiter').RateLimiter;
const request = require('request').defaults({strictSSL: false});

const base64Helper = require('./base64-helper.js');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

//todo discuss with John and rationalise error/failure messages
module.exports.RedLinkConsumer = function (config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const log = require('./log.js')(node).log;
    node.name = config.name;
    node.consumerStoreName = config.consumerStoreName;
    node.consumerMeshName = config.consumerMeshName;
    node.manualRead = config.manualReadReceiveSend;
    if (node.consumerMeshName) {
        node.consumerStoreName = node.consumerMeshName + ':' + node.consumerStoreName;
    }
    node.rateTypeReceiveSend = config.rateTypeReceiveSend;
    node.rateReceiveSend = config.rateReceiveSend;
    node.rateUnitsReceiveSend = config.rateUnitsReceiveSend;
    const rateType = node.manualRead ? 'none' : (node.rateTypeReceiveSend || 'none');
    const rate = node.rate || 1; //msg
    const nbRateUnits = node.rateReceiveSend || 1; //per
    const rateUnits = node.rateUnitsReceiveSend || 'second';
    let multiplier = Number(nbRateUnits) || 1;
    switch (rateUnits) {
        case 'second':
            multiplier *= 1000;
            break;
        case 'minute':
            multiplier *= 60 * 1000;
            break;
        case 'hour':
            multiplier *= 60 * 60 * 1000;
            break;
        case 'day':
            multiplier *= 24 * 60 * 60 * 1000;
    }
    multiplier /= (Number(rate) || 1);
    const limiter = new RateLimiter(1, multiplier);
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;

    function getNewNotify() {
        //check if the notify is for this consumer name with the registered store name
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' +
            node.name + '"' + ' AND notifySent NOT LIKE "%' + node.id + '%"';
        const notifies = alasql(notifiesSql);
        let newNotify = null;
        if (notifies.length > 0) {
            newNotify = notifies[notifies.length - 1];
        }
        return newNotify;
    }

    function updateNotifyTable(newNotify) {
        const existingNotifiedNodes = newNotify.notifySent.trim();
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const updateNotifySql = 'UPDATE notify SET notifySent="' + newNotifiedNodes + '" WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND storeName="' + node.consumerStoreName + '"';
        alasql(updateNotifySql);
    }

    alasql.fn[newMsgNotifyTrigger] = () => {
        // OK, this consumer will now add its own node.id to the notify trigger message since it comes in without one.
        const newNotify = getNewNotify();
        if (!newNotify) {
            return;
        }
        updateNotifyTable(newNotify);
        const notifyMessage = {
            redlinkMsgId: newNotify.redlinkMsgId,
            notifyType: 'producerNotification',
            src: {storeName: newNotify.storeName, address: newNotify.srcStoreIp + ':' + newNotify.srcStorePort,},
            dest: {storeName: newNotify.storeName, serviceName: newNotify.serviceName, consumer: node.id}
        };

        if (node.manualRead) {
            sendMessage({notify: notifyMessage});
        } else {
            sendMessage({notify: notifyMessage}); //send notify regardless of whether it is manual or auto read
            //todo check if rateType is none
            if (rateType === 'none') {
                readMessage(notifyMessage.redlinkMsgId).then(response => {
                    sendMessage(response);
                }).catch(err => {
                    sendMessage(err);
                });
            } else {
                limiter.removeTokens(1, function (err, remainingRequests) {
                    readMessage(notifyMessage.redlinkMsgId).then(response => {
                        sendMessage(response);
                    }).catch(err => {
                        sendMessage(err);
                    });
                });
            }
        }
    };

    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId + ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    alasql(createTriggerSql);

    //localStoreConsumers (storeName STRING, serviceName STRING)'); 
    //can have multiple consumers with same name registered to the same store
    const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '")';
    alasql(insertIntoConsumerSql);

    node.on('close', (removed, done) => {
        //todo deregister this consumer
        const dropNotifyTriggerSql = 'DROP TRIGGER ' + msgNotifyTriggerId; //todo this wont work- see https://github.com/agershun/alasql/issues/1113
        //clean up like in the redlinkStore- reinit trigger function to empty
        alasql(dropNotifyTriggerSql);
//        log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName + +'"' + 'AND serviceName="' + node.name + '"';
        alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
//        log('removed consumer from local store...');
        //TODO use the getlocalNorthSouthConsumers function
        const localConsumersSql = 'SELECT * FROM localStoreConsumers';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers';
        const localConsumers = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        done();
    });

    node.on("input", msg => {
        if (msg.cmd === 'read') {
            if (node.manualRead) {
                if (msg.redlinkMsgId) {
                    readMessage(msg.redlinkMsgId).then(response => {
                        sendMessage(response);
                    }).catch(err => {
                        sendMessage(err);
                    });
                } else {// should be here for a normal read
                    try {
                        readMessageWithoutId();
                    } catch (e) {
                        console.log(e); //shouldn't happen
                    }
                }
            } //manual read
        }  //cmd read
        else {  // Reply message, this is where the reply is actually sent back to the replyMessages on the Producer.
            if (msg.redlinkMsgId && !msg.sendOnly) { //todo delete notify if sendOnly
                const notifiesSql = 'SELECT redlinkMsgId from notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '" and storeName="' + node.consumerStoreName + '"  and notifySent = "' + node.id + '"';
                const notifies = alasql(notifiesSql);
                const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + msg.redlinkMsgId + '"';
                const matchingMessages = alasql(msgSql);
                sendMessage({debug: {action: 'replySend', direction: 'inBound', message: matchingMessages}});
                // node.send([]);
                if (matchingMessages.length > 0) { //should have only one
                    const replyStore = matchingMessages[0].storeName;
                    const replyService = matchingMessages[0].serviceName;
                    const redlinkProducerId = matchingMessages[0].redlinkProducerId;
                    const notifySql = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"and notifySent LIKE "%' + node.id + '%"';
                    const notifies = alasql(notifySql); //should have only one

                    if (notifies.length > 0) {
                        const replyAddress = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
                        //                 delete msg.preserved;
                        const body = {
                            replyingService: replyService,
                            redlinkMsgId: msg.redlinkMsgId,
                            redlinkProducerId: redlinkProducerId,
                            payload: base64Helper.encode(msg.payload)
                        };
                        const options = {
                            method: 'POST',
                            url: 'https://' + replyAddress + '/reply-message',
                            body,
                            json: true
                        };
                        request(options, function (error, response) {
                            body.payload = base64Helper.decode(body.payload);
                        });
                    }
                }
                // OK, I have completed the whole job and sent the reply, now to finally remove the original Notifiy for thi job.
                deleteNotify(msg.redlinkMsgId);
            }
        }
    });

    function sendMessage(msg) {
        node.send([msg.receive, msg.notify, msg.failure, msg.debug]);
    }

    function readMessageWithoutId() { //will attempt to read the first available message for this consumer- if it gets a 404, will attempt to read the next message
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '"  and notifySent LIKE "%' + node.id + '%" and read=' + false;
        const notifies = alasql(notifiesSql);
        if (notifies.length > 0) {
            return readMessage(notifies[0].redlinkMsgId).then(msg => {
                sendMessage(msg);
            }).catch(err => {
                sendMessage({debug: err});
                readMessageWithoutId();
            })
        } else {
            sendMessage({failure: {error: 'Store ' + node.consumerStoreName + ' does not have notifies for consumer ' + node.name + ' with nodeId ' + node.id}});
        }
    }

    function deleteNotify(redlinkMsgId) {
        const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' + redlinkMsgId + '" and storeName = "' + node.consumerStoreName + '" and notifySent LIKE "%' + node.id + '%"';
        const deleteNotify = alasql(deleteNotifyMsg);
    }

    function readMessage(redlinkMsgId) {
        return new Promise((resolve, reject) => {
            let notifiesSql;
            if (redlinkMsgId) {
                notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND redlinkMsgId="' + redlinkMsgId + '" AND notifySent LIKE "%' + node.id + '%"';
            } else {
                notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '"  and notifySent LIKE "%' + node.id + '%"';
            }
            const notifies = alasql(notifiesSql);
            if (notifies.length > 0) {
                const sendingStoreName = notifies[0].storeName;
                const address = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
                const options = {
                    method: 'POST',
                    url: 'https://' + address + '/read-message',
                    body: {redlinkMsgId},
                    json: true
                };
                sendMessage({debug: {"debugData": "storeName " + sendingStoreName + ' ' + node.name + "action:consumerRead" + options}});
                //            node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'outBound',Data:options},null]);
                const updateNotifyStatus = 'UPDATE notify SET read=' + true + ' WHERE redlinkMsgId="' + redlinkMsgId + '"  and notifySent LIKE "%' + node.id + '%"';
                alasql(updateNotifyStatus);
                request(options, function (error, response) {
                    if (response && response.statusCode === 200) {
                        if (response.body.message) {
                            response.body.message = base64Helper.decode(response.body.message);
                        }
                        const msg = response.body;
                        if (msg) {
                            msg.payload = msg.message.payload;
                            delete msg.preserved;
                            delete msg.message;
                            delete msg.read;
                            const receiveMsg = {
                                redlinkMsgId,
                                msg,
                                storeName: sendingStoreName,
                                consumerName: node.name,
                                action: 'consumerRead',
                                direction: 'inBound',
                                error: false
                            };
                            resolve({receive: receiveMsg});
                            if (msg.sendOnly) {
                                deleteNotify(redlinkMsgId);
                            }
                        } else {
                            reject({failure: {error: 'Empty response got when reading message'}});
                            deleteNotify(redlinkMsgId);
                        }
                    } else { //not 200- error case
                        if (response.body.message) {
                            response.body.message = base64Helper.decode(response.body.message);
                        }
                        const msg = response.body;
                        if (msg) {
                            // OK the store has told me the message is no longer available, so I will remove this notify
                            const errorMessage = {
                                storeName: sendingStoreName,
                                consumerName: node.name,
                                action: 'consumerRead',
                                direction: 'inBound',
                                msg: msg,
                                redlinkMsgId: redlinkMsgId,
                                error: true
                            };
                            reject({failure: errorMessage});
                            deleteNotify(redlinkMsgId);
                        }
                    }
                }); //request
            } else {
                reject({failure: 'Store ' + node.consumerStoreName + ' does not have notifies for id ' + redlinkMsgId + ' and consumer nodeId ' + node.id});
                //no notify in the first place- need not call deleteNotify
            } //notifies
        })
    } //readMessage
};