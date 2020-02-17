const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});
const stream = require('stream');

const base64Helper = require('./base64-helper.js');
const rateLimiterProvider = require('./rateLimiter.js');
const redlinkConstants = require('./redlinkConstants');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkConsumer = function (config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.name = config.name;
    node.reSyncTime = 10000; // This timer defines the consumer store update sync for localStoreConsumers.
    node.reSyncTimerId = {};
    node.reNotifyTime = +(config.reNotifyInterval || 1)*1000; // This timer defines the consumer store update sync for localStoreConsumers reNotifies
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
        readMessage(redlinkMsgId).then(response => {
            sendMessage(response);
        }).catch(err => sendMessage(err));
    }


    function sendOutNotify() {
       const nResult = alasql('SELECT COUNT(notifySent) as myCount from notify  WHERE read=false and storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%"');
       if (nResult[0].myCount > 0){
         const data = alasql('SELECT * from notify  WHERE read=false and storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%"');
         const notifyMessage = {
               redlinkMsgId: data[0].redlinkMsgId,
               notifyType: 'producerNotification',
               src: {redlinkProducerId: data[0].redlinkProducerId, storeName: data[0].storeName, storeAddress: data[0].srcStoreAddress + ':' + data[0].srcStorePort},
               dest: {storeName: data[0].storeName, serviceName: data[0].serviceName, consumer: node.id},
               path: base64Helper.decode(data[0].notifyPath),
               notifyCount: nResult[0].myCount
           };
         if (node.manualRead) {
            sendMessage({notify: notifyMessage});
            }
          else 
            {
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
                notifyMessage.warning = 'inTransitLimit ' + node.inTransitLimit + ' exceeded. Notify for redlinkMsgId '+data[0].redlinkMsgId+' will be discarded';
                sendMessage({notify: notifyMessage});  // TODO- still send the notify out?
                deleteNotify(data[0].redlinkMsgId);
              }
          }
       }
    }


    function updateGlobalConsumerEcm(serviceName, consumerId, storeName, ecm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + storeName + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId + 
                                                                                    '" AND storeName="' + storeName + '"' ;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerEcm = 'UPDATE globalStoreConsumers SET ecm=' + ecm + ' WHERE localStoreName="' + storeName + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId + 
                                                                                     '" AND storeName="' + storeName + '"' ;
            alasql(updateConsumerEcm);
            }
    }


    function updateGlobalConsumerErm(serviceName, consumerId, storeName, erm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + storeName + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId + 
                                                                                    '" AND storeName="' + storeName + '"' ;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerErm = 'UPDATE globalStoreConsumers SET erm=' + erm + ' WHERE localStoreName="' + storeName + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId + 
                                                                                     '" AND storeName="' + storeName + '"' ;
            alasql(updateConsumerErm);
            }
    }



    alasql.fn[newMsgNotifyTrigger] = () => {
        // OK, this consumer will now add its own node.id to the notify.notifySent trigger message since it comes in without one, unfortunately alasql does not send data with triggers.

        const notifyAndCount = getNewNotifyAndCount();
        if (!notifyAndCount) {
            return;
        }
      else
        {
          const newNotify = notifyAndCount.notify;
          updateNotifyTable(newNotify);
          sendOutNotify();
        }
    };
    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId + ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    alasql(createTriggerSql);
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
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName +'"' + ' AND serviceName="' + node.name + '" AND consumerId="' + node.id + '"';
        alasql(deleteConsumerSql);
        done();
    });

    function dropTrigger(triggerName) { // workaround for https://github.com/agershun/alasql/issues/1113
        //todo this issue has been fixed- update to new version of alasql and fix this
        alasql.fn[triggerName] = () => {
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
           if (sResult.length === 0) {
             const deleteFromConsumerSql = 'DELETE FROM localStoreConsumers where consumerId = "'+ node.id +'"';
             const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '","' + node.id +'")';
             alasql(deleteFromConsumerSql);
             alasql(insertIntoConsumerSql);
           }
        },timeOut);
    }

    function reNotifyConsumers(timeOut) {
        return setInterval(function () {
            const nResult = alasql('SELECT COUNT(notifySent) as myCount from notify  WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent LIKE "%' + node.id + '%" AND read=false');
            node.status({fill: "green", shape: "dot", text: 'N:' + nResult[0].myCount});
            if (nResult[0].myCount > 0) {
                const data = alasql('SELECT * from notify  WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '" and notifySent LIKE "%' + node.id + '%" AND read=false');
                const notifyMessage = {
                    redlinkMsgId: data[0].redlinkMsgId,
                    notifyType: 'producerNotification',
                    src: {storeName: data[0].storeName, address: data[0].srcStoreAddress + ':' + data[0].srcStorePort,},
                    dest: {storeName: data[0].storeName, serviceName: data[0].serviceName, consumer: node.id},
                    path: base64Helper.decode(data[0].notifyPath),
                    notifyCount: nResult[0].myCount
                };
                if (node.manualRead) {
                    sendMessage({notify: notifyMessage});
                } else {
                    sendOutNotify();
                }
            }
        },timeOut);
    }

    node.on("input", msg => {
        const topic = msg.topic? msg.topic.toLowerCase(): '';
        if (topic === 'read' || topic === 'consumerread') {
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
            } else {
                sendMessage({failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to manually read message when consumer set to auto read', 'consumerRead')});
            }
        }  // cmd read
        
        else {  // Reply message, this is where the reply is actually sent back to the replyMessages on the Producer.
            if (msg.redlinkMsgId) {
                if(msg.sendOnly==='false') {
                    msg.sendOnly = false;
                }
                if (!msg.sendOnly) { // todo delete notify if sendOnly
                    const notifySql = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"and notifySent LIKE "%' + node.id + '%"';
                    const notifies = alasql(notifySql); // should have only one
                    if (notifies.length > 0) {
                        if (notifies[0].read === false) {// attempt to send reply before reading
                            sendMessage({
                                debug: {
                                    error: 'Attempt to reply to redlinkMsgId ' + msg.redlinkMsgId + ' before reading message'
                                },
                                failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to reply to message before reading message', 'consumerReply')
                            });
                            return;
                        }
                        const replyDelayCalc = Date.now()-notifies[0].notifyTime;
        
                        updateGlobalConsumerErm(node.name, node.id, node.consumerStoreName, replyDelayCalc); 

                        const replyService = notifies[0].serviceName;
                        const redlinkProducerId = notifies[0].redlinkProducerId;
                        let notifyPathIn = base64Helper.decode(notifies[0].notifyPath); //todo this part looks shady
                        let notifyPath;
                        // The first entry in the notify contains the enforceReversePath
                        // if (node.enforceReversePath) {
                        if (notifyPathIn[0].enforceReversePath) {
                            notifyPath = notifyPathIn.pop();
                        } else {
                            notifyPath = notifyPathIn[0];
                            notifyPathIn = [];
                        }
                        const replyAddress = notifyPath.address + ':' + notifyPath.port;
                        notifyPathIn = base64Helper.encode(notifyPathIn);
                        var cerror = '';
                        if (msg.cerror) {
                            cerror = base64Helper.encode(msg.cerror);
                        }
                        // delete msg.preserved;
                        const body = {
                            redlinkProducerId,
                            replyingService: replyService,
                            replyingServiceId: node.id,
                            replyingStoreName: msg.consumerStoreName,
                            redlinkMsgId: msg.redlinkMsgId,
                            replyDelay : replyDelayCalc,
                            payload: base64Helper.encode(msg.payload),
                            notifyPath: notifyPathIn,
                            cerror: cerror,
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
                    // OK, I have completed the whole job and sent the reply, now to finally remove the original Notifiy for this job.
                    deleteNotify(msg.redlinkMsgId);
                    watermark--;
                } else {
                    sendMessage({failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to reply to a sendOnly message', 'consumerReply')});
                }
            } else {
                sendMessage({failure: getFailureMessage('', 'Missing redlinkMsgId', 'consumerReply')});
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
            sendMessage({failure: getFailureMessage(null, 'Store ' + node.consumerStoreName + ' does not have notifies for this consumer', 'consumerReadWithoutId')});
        }
    }

    function deleteNotify(redlinkMsgId) {
        const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' + redlinkMsgId + '" and storeName = "' + node.consumerStoreName + '" and notifySent LIKE "%' + node.id + '%"';
        return alasql(deleteNotifyMsg);
    }

    function getMessageMetadataFromHeaders(headers) {
        const metadata = {};
        for (const property in redlinkConstants.messageFields) { //key- lowercase, value- camelCase
            if (headers[property]) {
                metadata[redlinkConstants.messageFields[property]] = headers[property];
            }
        }
        if (Object.keys(metadata).length === 0)
            return undefined;
        return metadata;
    }

    function getPayload(res) {
        return new Promise((resolve, reject)=>{
            if(res instanceof stream.Stream){
                let data = "";
                res.on("data", chunk => data += chunk);
                res.on("end", () => resolve(base64Helper.decode(data)));
                res.on("error", error => reject(error));
            }else{
                resolve(base64Helper.decode(res));
            }
        });
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
                let notifyPath; //todo revisit this- code duplication
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
                const readDelayCalc =  Date.now()-notifies[0].notifyTime;

                // Update My Consumers Read Stats
                updateGlobalConsumerEcm(node.name, node.id, node.consumerStoreName, readDelayCalc);
                updateGlobalConsumerErm(node.name, node.id, node.consumerStoreName, readDelayCalc); 

                const options = {
                    method: 'POST',
                    url: 'https://' + address + '/read-message',
                    body: {redlinkMsgId, notifyPath:notifyPathIn, redlinkProducerId: notifies[0].redlinkProducerId, consumerId:node.id, consumerService: node.name, consumerStoreName: node.consumerStoreName, readDelay : readDelayCalc},
                    json: true
                };
                sendMessage({debug: {"debugData": `storeName ${sendingStoreName} ${node.name}action:consumerRead${options}`}});
                const updateNotifyStatus = 'UPDATE notify SET read=' + true + ' WHERE redlinkMsgId="' + redlinkMsgId + '"  and notifySent LIKE "%' + node.id + '%"';
                alasql(updateNotifyStatus);
                request(options, function (error, response) {
                    if (response && response.statusCode === 200) {
                        const msg = getMessageMetadataFromHeaders(response.headers);
                        if (msg) {
                            let retrieveDelay;
                            if(msg.timestamp){
                                retrieveDelay = (Date.now()-msg.timestamp);
                            }
                            delete msg.preserved;
                            delete msg.message;
                            delete msg.read;
                            const receiveMsg = {
                                redlinkMsgId,
                                consumerName: node.name,
                                consumerId: node.id,
                                consumerStoreName: sendingStoreName,
                                action: 'consumerRead',
                                direction: 'inBound',
                                redlinkProducerId: msg.redlinkProducerId,
                                producerStoreName: msg.storeName,
                                sendOnly: msg.sendOnly,
                                error: false,
                                notifyReadDelay : readDelayCalc,
                                readTransport : retrieveDelay - readDelayCalc
                            };
                            if (response.body) {
                                getPayload(response.body).then(payload => {
                                    receiveMsg.payload = payload.payload ? payload.payload : payload;
                                    resolve({receive: receiveMsg});
                                    if (msg.sendOnly === 'false') {
                                        msg.sendOnly = false;
                                    }
                                    if (msg.sendOnly) {
                                        deleteNotify(redlinkMsgId);
                                    } else {
                                        watermark++;
                                    }
                                }).catch(err => {
                                    const errorDesc = `Error reading payload from store at :${address} ${err}`;
                                    reject({failure: getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead')});
                                    deleteNotify(redlinkMsgId);
                                });
                            }
                        } else {
                            const errorDesc = 'Message metadata not received';
                            reject({failure: getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead')});
                            deleteNotify(redlinkMsgId);
                        }
                    } else { // not 200- error case
                        if (response && response.body && response.body.message) {
                            response.body.message = base64Helper.decode(response.body.message);
                        }
                        const statusCode = response && response.statusCode;
                        const body = response && response.body;
                        // OK the store has told me the message is no longer available, so I will remove this notify
                        let errorDesc;
                        let producerId;
                        if (error) {
                            try {
                                errorDesc = JSON.stringify(error);
                            } catch (e) {
                                errorDesc = error;
                            }
                        } else if (body) {
                            errorDesc = body.err ? body.err : body;
                            producerId = body.redlinkProducerId;
                        }
                        const failureMessage = getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead');
                        failureMessage.statusCode = statusCode;
                        failureMessage.redlinkProducerId = producerId;
                        reject({failure: failureMessage});
                        deleteNotify(redlinkMsgId);
                    }
                }); // request
            } else {
                const errorDesc = `Store ${node.consumerStoreName} does not have notifies for msgid ${redlinkMsgId} and this consumer`;
                reject({failure: getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead')});
                // no notify in the first place- need not call deleteNotify
            } // notifies
        })
    } // readMessage

    function getFailureMessage(redlinkMsgId, errorDesc, action) {
        return {
            storeName: node.consumerStoreName,
            consumerName: node.name,
            consumerId: node.id,
            action,
            error: errorDesc,
            redlinkMsgId
        }
    }
};


