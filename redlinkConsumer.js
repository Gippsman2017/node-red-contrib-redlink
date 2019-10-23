const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});

const base64Helper = require('./base64-helper.js');
const rateLimiterProvider = require('./rateLimiter.js');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

// todo discuss with John and rationalise error/failure messages
module.exports.RedLinkConsumer = function (config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const log = require('./log.js')(node).log;
    node.name = config.name;

    node.reSyncTime = 10000; // This timer defines the consumer store update sync for localStoreConsumers.
    node.reSyncTimerId = {};

    node.reNotifyTime = 1000; // This timer defines the consumer store update sync for localStoreConsumers reNotifies
    node.reNotifyTimerId = {};

    node.enforceReversePath = false;
    node.consumerStoreName = config.consumerStoreName;
    node.manualRead = config.manualReadReceiveSend;
    node.inTransitLimit = config.intransit;
    let watermark = 0;
    node.rateTypeReceiveSend = config.rateTypeReceiveSend;
    node.rateReceiveSend = config.rateReceiveSend;
    node.rateUnitsReceiveSend = config.rateUnitsReceiveSend;
    const rateType = node.manualRead ? 'none' : (node.rateTypeReceiveSend || 'none');
    const limiter = rateType==='none' ? null: rateLimiterProvider.getRateLimiter(config.rateReceiveSend, config.nbRateUnitsReceiveSend, config.rateUnitsReceiveSend);
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;

    function getNewNotifyAndCount() {
        // check if the notify is for this consumer name with the registered store name
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' +  node.name + '"' + ' AND notifySent NOT LIKE "%' + node.id + '%"';
        const notifies    = alasql(notifiesSql);
        let newNotify     = null;
        const notifiesSqlCount = alasql('SELECT COUNT(DISTINCT redlinkMsgId) from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"');
        if (notifies.length > 0) {
            newNotify = {notify: notifies[notifies.length - 1], notifyCount: notifiesSqlCount[0]['COUNT(DISTINCT redlinkMsgId)']};
        }
        return newNotify;
    }

    function updateNotifyTable(newNotify) {
        const existingNotifiedNodes = newNotify.notifySent.trim();
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const updateNotifySql = 'UPDATE notify SET notifySent="' + newNotifiedNodes + '" WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND storeName="' + node.consumerStoreName + '"';
        alasql(updateNotifySql);
    }

    function readMessageAndSendToOutput(redlinkMsgId) {
        readMessage(redlinkMsgId).then(response => sendMessage(response)).catch(err => sendMessage(err));
    }


    function sendOutNotify() {
       const nResult = alasql('SELECT COUNT(notifySent) as myCount from notify  WHERE read=false and storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%"');
       if (nResult[0].myCount > 0){
         const data = alasql('SELECT * from notify  WHERE read=false and storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%"');
         const notifyMessage = {
            redlinkMsgId: data[0].redlinkMsgId,
            notifyType: 'producerNotification',
            src: {storeName: data[0].storeName, address: data[0].srcStoreAddress + ':' + data[0].srcStorePort,},
            dest: {storeName: data[0].storeName, serviceName: data[0].serviceName, consumer: node.id},
            path: base64Helper.decode(data[0].notifyPath),
            notifyCount:nResult[0].myCount
            };
         if (node.manualRead) {
            sendMessage({notify: notifyMessage});
            }
          else {
              // check inTransitLimit first
              if (watermark < node.inTransitLimit) {
                   sendMessage({notify: notifyMessage}); // send notify regardless of whether it is manual or auto read
                   // todo read oldest message first- right now it reads the notifies in random order- see getNewNotifyAndCount() at beginning of trigger
                   if (limiter === null) {
                       readMessageAndSendToOutput(notifyMessage.redlinkMsgId);
                   } else 
                   {
                       limiter.removeTokens(1, function (err, remainingRequests) {
                           readMessageAndSendToOutput(notifyMessage.redlinkMsgId);
                      });
                   }
              } 
            else 
              {
                notifyMessage.warning = 'inTransitLimit ' + node.inTransitLimit + ' exceeded. This notify will be discarded';
                sendMessage({notify: notifyMessage});  // TODO- still send the notify out?
                const deleteNotify1 = deleteNotify(data[0].redlinkMsgId);
              }
          }
 
       }
    }

    alasql.fn[newMsgNotifyTrigger] = () => {
        // OK, this consumer will now add its own node.id to the notify trigger message since it comes in without one.
        const notifyAndCount = getNewNotifyAndCount();
        if (!notifyAndCount) {
            return;
        }
        const newNotify = notifyAndCount.notify;
        const notifyCount = notifyAndCount.notifyCount;
        updateNotifyTable(newNotify);
        sendOutNotify();
    };


    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId + ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    alasql(createTriggerSql);

    // localStoreConsumers (storeName STRING, serviceName STRING)'); 
    // can have multiple consumers with same name registered to the same store
    const deleteFromConsumerSql = 'DELETE FROM localStoreConsumers where consumerId = "'+ node.id +'"';
    const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '","' + node.id +'")';
    alasql(deleteFromConsumerSql);
    alasql(insertIntoConsumerSql);

    const selectResult = alasql('SELECT storeName from stores WHERE storeName = "' + node.consumerStoreName + '"');
    if (selectResult.length > 0) {
       node.status({fill: "green", shape: "dot", text: 'Conn: '+node.consumerStoreName});
      } else{
       node.status({fill: "red", shape: "dot", text:'Error: No '+node.consumerStoreName});
    }

    node.reSyncTimerId = reSyncStores(node.reSyncTime); // This is the main call to sync this consumer with its store on startup and it also starts the interval timer.
    node.reNotifyTimerId = reNotifyConsumers(node.reNotifyTime); // This is the main call to sync this consumer with its store on startup and it also starts the interval timer.

    node.on('close', (removed, done) => {
        clearInterval(node.reSyncTimerId);
        // clean up like in the redlinkStore- reinit trigger function to empty
        clearInterval(node.reNotifyTimerId);
        dropTrigger(msgNotifyTriggerId);
        // log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName +'"' + ' AND serviceName="' + node.name + '" AND consumerId="' + node.id + '"';
        alasql(deleteConsumerSql);
        done();
    });

    function dropTrigger(triggerName) { // workaround for https://github.com/agershun/alasql/issues/1113
        alasql.fn[triggerName] = () => {
            // console.log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
        }
    }

    function reSyncStores(timeOut) {
        return setInterval(function () {
           // First get any local consumers that I own and update my own global entries in my own store, this updates ttl.
           const sResult = alasql('SELECT storeName from stores WHERE storeName = "' + node.consumerStoreName + '"');
           if (sResult.length > 0) {
             node.status({fill: "green", shape: "dot", text: ''});
           } else {
             node.status({fill: "red",    shape: "dot", text: 'Error: No Store:'+node.consumerStoreName});
           }
           if (sResult.length == 0) {
             const deleteFromConsumerSql = 'DELETE FROM localStoreConsumers where consumerId = "'+ node.id +'"';
             const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '","' + node.id +'")';
             alasql(deleteFromConsumerSql);
             alasql(insertIntoConsumerSql);
           }
        },timeOut);    
    }

    function reNotifyConsumers(timeOut) {
        return setInterval(function () {
           const nResult = alasql('SELECT COUNT(notifySent) as myCount from notify  WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%"');
            node.status({fill: "green", shape: "dot", text: 'N:'+nResult[0].myCount});
           if (nResult[0].myCount > 0){
             const data = alasql('SELECT * from notify  WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '" and notifySent="'+node.id+'"');
             const notifyMessage = {
                redlinkMsgId: data[0].redlinkMsgId,
                notifyType: 'producerNotification',
                src: {storeName: data[0].storeName, address: data[0].srcStoreAddress + ':' + data[0].srcStorePort,},
                dest: {storeName: data[0].storeName, serviceName: data[0].serviceName, consumer: node.id},
                path: base64Helper.decode(data[0].notifyPath),
                notifyCount:nResult[0].myCount
            };

            if (node.manualRead) {
              sendMessage({notify: notifyMessage});
            }
          else
            {
              sendOutNotify();
            }   
          }
        },timeOut);    
    }


    node.on("input", msg => {
        if (msg.topic === 'read' || msg.topic === 'consumerRead') {
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
                        console.log(e); // shouldn't happen
                    }
                }
            } // manual read
        }  // cmd read
        else {  // Reply message, this is where the reply is actually sent back to the replyMessages on the Producer.
            if (msg.redlinkMsgId && !msg.sendOnly) { // todo delete notify if sendOnly
                if (/*matchingMessages.length > 0*/true) { // should have only one
                    const notifySql = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"and notifySent LIKE "%' + node.id + '%"';
                    const notifies = alasql(notifySql); // should have only one
                    if (notifies.length > 0) {
                        if(notifies[0].read===false){// attempt to send reply before reading
                            sendMessage({
                                debug: {
                                    error: 'Attempt to reply to redlinkMsgId ' + msg.redlinkMsgId + ' before reading message'
                                }
                            });
                            return;
                        }
                        const replyService = notifies[0].serviceName;
                        const redlinkProducerId = notifies[0].redlinkProducerId;

                        let notifyPathIn = base64Helper.decode(notifies[0].notifyPath);
                        // console.log('Consumer=',notifyPathIn);
                        let notifyPath = [];
                        // The first entry in the notify contains the enforseReversePath
                        // if (node.enforceReversePath) {
                        if (notifyPathIn[0].enforceReversePath) {
                           notifyPath=notifyPathIn.pop();
                        } else {
                           notifyPath=notifyPathIn[0];
                           notifyPathIn =[];
                        }   
                        
                        const replyAddress = notifyPath.address+':'+notifyPath.port;
                        notifyPathIn = base64Helper.encode(notifyPathIn);
                        // delete msg.preserved;
                        const body = {
                            redlinkProducerId,
                            replyingService: replyService,
                            redlinkMsgId: msg.redlinkMsgId,
                            payload: base64Helper.encode(msg.payload),
                            notifyPath: notifyPathIn
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
                // OK, I have completed the whole job and sent the reply, now to finally remove the original Notifiy for this job.
                deleteNotify(msg.redlinkMsgId);
                watermark--;
            }
        }
    });

    function sendMessage(msg) {
        node.send([msg.receive, msg.notify, msg.failure, msg.debug]);
    }

    function readMessageWithoutId() { // will attempt to read the first available message for this consumer- if it gets a 404, will attempt to read the next message
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
        return deleteNotify;
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

                let notifyPathIn = base64Helper.decode(notifies[0].notifyPath);

                let notifyPath = [];
                // The first entry in the notify contains the enforseReversePath
                if (notifyPathIn[0].enforceReversePath) {
                   notifyPath=notifyPathIn.pop();
                } else {
                   notifyPath=notifyPathIn[0];
                   notifyPathIn =[];
                }   

                const address = notifyPath.address+':'+notifyPath.port;
                notifyPathIn = base64Helper.encode(notifyPathIn);
                const sendingStoreName = notifies[0].storeName;
                const options = {
                    method: 'POST',
                    url: 'https://' + address + '/read-message',
                    body: {redlinkMsgId, notifyPath:notifyPathIn},
                    json: true
                };
                sendMessage({debug: {"debugData": "storeName " + sendingStoreName + ' ' + node.name + "action:consumerRead" + options}});
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
                                consumerName: node.name,
                                localStoreName: sendingStoreName,
                                action: 'consumerRead',
                                direction: 'inBound',
                                redlinkProducerId: msg.redlinkProducerId,
                                producerStoreName: msg.storeName,
                                sendOnly: msg.sendOnly,
                                payload: msg.payload,
                                error: false
                            };
                            resolve({receive: receiveMsg});
                            if (msg.sendOnly) {
                                deleteNotify(redlinkMsgId);
                            } else {
                                watermark++;
                            }
                        } else {
                            reject({failure: {error: 'Empty response got when reading message'}});
                            deleteNotify(redlinkMsgId);
                        }
                    } else { // not 200- error case
                        if (response && response.body && response.body.message) {
                            response.body.message = base64Helper.decode(response.body.message);
                        }
                        const msg = response ? response.body : null;
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
                }); // request
            } else {
                reject({failure: 'Store ' + node.consumerStoreName + ' does not have notifies for id ' + redlinkMsgId + ' and consumer nodeId ' + node.id});
                // no notify in the first place- need not call deleteNotify
            } // notifies
        })
    } // readMessage
};
