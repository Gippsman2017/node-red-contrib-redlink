
const alasql  = require('alasql');
const request = require('request').defaults({strictSSL: false});
const stream  = require('stream');

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
    node.reNotifyTime = +(config.reNotifyInterval || 1) * 1000; // This timer defines the consumer store update sync for localStoreConsumers reNotifies
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
    const limiter = rateType === 'none' ? null : rateLimiterProvider.getRateLimiter(config.rateReceiveSend, config.nbRateUnitsReceiveSend, config.rateUnitsReceiveSend);
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;

    function updateStatusCounter() {
        const nResult = alasql(`SELECT COUNT(consumerId) as myCount from notify  WHERE storeName="${node.consumerStoreName}" AND serviceName="${node.name}" AND consumerId= "${node.id}"  and read=false`);
        node.status({fill: "green", shape: "dot", text: 'N:' + nResult[0].myCount});
        return nResult[0].myCount;
    }

    function getNewNotify() {
        // check if the notify is for this consumer name with the registered store name and there is NOT a notify that has been read and in progress by this consumer
        const notifies = alasql(`SELECT count(*) as notifyCount from notify WHERE storeName="${node.consumerStoreName}" AND serviceName="${node.name}" AND consumerId = "${node.id}" and not notifySent = "" `)[0].notifyCount;
        return Boolean(notifies);
         }

    function updateNotifyTable(newNotify) {
        const existingNotifiedNodes = newNotify.notifySent;
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const updateNotifySql = `UPDATE notify SET notifySent="${newNotifiedNodes}" WHERE redlinkMsgId="${newNotify.redlinkMsgId}" AND storeName="${node.consumerStoreName}" AND consumerId = "${node.id}"`;
        const result = alasql(updateNotifySql);
    }

    function readMessageAndSendToOutput(redlinkMsgId) { //todo use async await
        readMessage(redlinkMsgId).then(response => sendMessage(response)).catch(err => sendMessage(err));
    }

    function sendOutNotify() {
        const notifyCount = updateStatusCounter();
        if (notifyCount > 0) {
            const data = alasql(`SELECT * from notify  WHERE read=false and storeName="${node.consumerStoreName}" AND serviceName="${node.name}" AND consumerId="${node.id}" AND  notifySent = "" order by notifyTime limit 1`);
            if (data.length === 1) {
                updateNotifyTable(data[0]);
                const notifyMessage = {
                    redlinkMsgId: data[0].redlinkMsgId,
                    notifyType: 'producerNotification',
                    src: {
                        redlinkProducerId: data[0].redlinkProducerId,
                        storeName: data[0].srcStoreName,
                        storeAddress: data[0].srcStoreAddress + ':' + data[0].srcStorePort
                    },
                    dest: {storeName: data[0].storeName, serviceName: data[0].serviceName, consumer: node.id},
                    path: base64Helper.decode(data[0].notifyPath),
                    notifyCount: notifyCount
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
                        if (limiter === null) { readMessageAndSendToOutput(notifyMessage.redlinkMsgId); } 
                                        else  { limiter.removeTokens(1, function (err, remainingRequests) { readMessageAndSendToOutput(notifyMessage.redlinkMsgId); }); }
                    } 
                  else 
                    { //This shouldnt happen
                        notifyMessage.warning = `inTransitLimit ${node.inTransitLimit} exceeded. Notify for redlinkMsgId ${data[0].redlinkMsgId} will be discarded`;
                        sendMessage({notify: notifyMessage});  // TODO- still send the notify out?
                        deleteNotify(data[0].redlinkMsgId);
                    }
                }
            }
        }
    }

    function calculateEnm(serviceName, consumerId) {
        const existingLocalConsumerSql = `SELECT * FROM localStoreConsumers   WHERE storeName="${node.consumerStoreName}" AND serviceName="${serviceName}"`;
        const notifyCountSql = `select count(*) nc from (SELECT * FROM notify WHERE storeName="${node.consumerStoreName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}")`;
        const nc = alasql(notifyCountSql)[0].nc;
        const itlSql = `select inTransitLimit itl from (${existingLocalConsumerSql})`;
        const itl = alasql(itlSql)[0].itl;
        return ((nc) / itl * 100).toFixed(0);
    }

    function updateGlobalConsumerEnm(serviceName, consumerId, storeName, enm) {
        const existingGlobalConsumerSql = `SELECT * FROM globalStoreConsumers WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerEnm = `UPDATE globalStoreConsumers SET enm=${enm} WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
            alasql(updateConsumerEnm);
        }
    }

    function updateGlobalConsumerEcm(serviceName, consumerId, storeName, ecm) {
        const existingGlobalConsumerSql = `SELECT * FROM globalStoreConsumers WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerEcm = `UPDATE globalStoreConsumers SET ecm=${ecm} WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
            alasql(updateConsumerEcm);
        }
    }


    function updateGlobalConsumerErm(serviceName, consumerId, storeName, erm) {
        const existingGlobalConsumerSql = `SELECT * FROM globalStoreConsumers WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerErm = `UPDATE globalStoreConsumers SET erm=${erm} WHERE localStoreName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}"`;
            alasql(updateConsumerErm);
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    alasql.fn[newMsgNotifyTrigger] = () => {
        // OK, this consumer will now add its own node.id to the notify.notifySent trigger message since it comes in without one, unfortunately alasql does not send data with triggers.
       if (!getNewNotify()) { sendOutNotify(); }
    };
    // ---------------------------------------------------------------------------------------------------------

    const createTriggerSql = `CREATE TRIGGER ${msgNotifyTriggerId} AFTER INSERT ON notify CALL ${newMsgNotifyTrigger}()`;
    alasql(createTriggerSql);
    // can have multiple consumers with same name registered to the same store
    const deleteFromConsumerSql = `DELETE FROM localStoreConsumers where consumerId = "${node.id}"`;
    const insertIntoConsumerSql = `INSERT INTO localStoreConsumers ("${node.consumerStoreName}","${node.name}","${node.id}",${node.inTransitLimit})`;
    alasql(deleteFromConsumerSql);
    alasql(insertIntoConsumerSql);

    const selectResult = alasql(`SELECT storeName from stores WHERE storeName = "${node.consumerStoreName}"`);
    if (selectResult.length > 0) { node.status({fill: "green", shape: "dot", text: 'Conn: ' + node.consumerStoreName}); } 
                            else { node.status({fill: "red", shape: "dot", text: 'Error: No ' + node.consumerStoreName}); }

    node.reSyncTimerId   = reSyncStores(node.reSyncTime); // This is the main call to sync this consumer with its store on startup and it also starts the interval timer.
    node.reNotifyTimerId = reNotifyConsumers(node.reNotifyTime); // This is the main call to sync this consumer with its store on startup and it also starts the interval timer.

    node.on('close', (removed, done) => {
        clearInterval(node.reSyncTimerId);
        // clean up like in the redlinkStore- reinit trigger function to empty
        clearInterval(node.reNotifyTimerId);
        dropTrigger(msgNotifyTriggerId);
        const deleteConsumerSql = `DELETE FROM localStoreConsumers WHERE storeName="${node.consumerStoreName}" AND serviceName="${node.name}" AND consumerId="${node.id}"`;
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
            const sResult = alasql(`SELECT storeName from stores WHERE storeName = "${node.consumerStoreName}"`);
            if (sResult.length > 0) { updateStatusCounter(); } 
                               else { node.status({fill: "red", shape: "dot", text: `Error: Store Not Syncd Yet:${node.consumerStoreName}`}); }

            if (sResult.length === 0) {
                const deleteFromConsumerSql = `DELETE FROM localStoreConsumers where consumerId = "${node.id}"`;
                const insertIntoConsumerSql = `INSERT INTO localStoreConsumers ("${node.consumerStoreName}","${node.name}","${node.id}",${node.inTransitLimit})`;
                alasql(deleteFromConsumerSql);
                alasql(insertIntoConsumerSql);
            }
        }, timeOut);
    }

    function reNotifyConsumers(timeOut) {
        return setInterval(function () {
                 if (!getNewNotify()) { sendOutNotify(); }
                 const notifyCount = updateStatusCounter();
                 }, timeOut);
    }

    node.on("input", msg => {
        const topic = msg.topic ? msg.topic.toLowerCase() : '';
        if (topic === 'read' || topic === 'consumerread') {
            if (node.manualRead) {
                if (msg.redlinkMsgId) {
                    readMessage(msg.redlinkMsgId).then(response => {
                        sendMessage(response);
                    }).catch(err => {
                        sendMessage(err);
                    });
                } 
              else 
                {  // should be here for a normal read
                    try { readMessageWithoutId(); } 
                    catch (e) { console.log(e); }  // shouldn't happen
                }
            } 
          else 
            {
                sendMessage({failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to manually read message when consumer set to auto read', 'consumerRead')});
            }
        } 
      else 
        {  // Reply message, this is where the reply is actually sent back to the replyMessages on the Producer.
            if (msg.redlinkMsgId) {
                if (msg.sendOnly === 'false') {
                    msg.sendOnly = false;
                    if (!getNewNotify()) { sendOutNotify(); }
                }
                if (!msg.sendOnly) { // todo delete notify if sendOnly
                    const notifySql = `SELECT * FROM notify WHERE storeName="${node.consumerStoreName}"  and redlinkMsgId="${msg.redlinkMsgId}"and consumerId = "${node.id}" limit 1`;
                    const notifies = alasql(notifySql); // should have only one
                    if (notifies.length > 0) {
                        if (notifies[0].read === false) {// attempt to send reply before reading
                            sendMessage({
                                debug: { error: 'Attempt to reply to redlinkMsgId ' + msg.redlinkMsgId + ' before reading message' },
                                failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to reply to message before reading message', 'consumerReply')
                            });
                            return;
                        }
                        const replyDelayCalc = Date.now() - notifies[0].notifyTime;
                        // OK, I have completed the whole job and sent the reply, now to finally remove the original Notifiy for this job.
                        // Then recalculate the enm before replying with it.
                        deleteNotify(msg.redlinkMsgId);
                        updateGlobalConsumerEnm(node.name, node.id, node.consumerStoreName, calculateEnm(node.name, node.id));
                        updateGlobalConsumerErm(node.name, node.id, node.consumerStoreName, replyDelayCalc);
                        const replyService = notifies[0].serviceName;
                        const redlinkProducerId = notifies[0].redlinkProducerId;
                        let notifyPathIn = base64Helper.decode(notifies[0].notifyPath); //todo this part looks shady
                        let notifyPath;
                        // The first entry in the notify contains the enforceReversePath
                        // if (node.enforceReversePath) {
                        if (notifyPathIn[0].enforceReversePath) {
                            notifyPath = notifyPathIn.pop();
                        } 
                      else 
                        {
                            notifyPath = notifyPathIn[0];
                            notifyPathIn = [];
                        }
                        const replyAddress = notifyPath.address + ':' + notifyPath.port;
                        notifyPathIn = base64Helper.encode(notifyPathIn);
                        var cerror = '';
                        if (msg.cerror) { cerror = base64Helper.encode(msg.cerror); }
                        // delete msg.preserved;
                        const body = {
                            redlinkProducerId,
                            replyingService: replyService,
                            replyingServiceId: node.id,
                            replyingStoreName: msg.consumerStoreName,
                            redlinkMsgId: msg.redlinkMsgId,
                            replyDelay: replyDelayCalc,
                            payload: base64Helper.encode(msg.payload),
                            notifyPath: notifyPathIn,
                            enm: calculateEnm(node.name, node.id),
                            cerror: cerror,
                        };
                        const options = { method: 'POST', url: 'https://' + replyAddress + '/reply-message', body, json: true };
                        request(options, function (error, response) { body.payload = base64Helper.decode(body.payload); });
                        if (!getNewNotify()) { sendOutNotify(); }
                    }
                    watermark--;
                } 
              else 
                {
                  sendMessage({failure: getFailureMessage(msg.redlinkMsgId, 'Attempt to reply to a sendOnly message', 'consumerReply')});
                }
            } 
          else 
            {
              sendMessage({failure: getFailureMessage('', 'Missing redlinkMsgId', 'consumerReply')});
            }
            updateStatusCounter();
        }
    });

    function sendMessage(msg) {
        node.send([msg.receive, msg.notify, msg.failure, msg.debug]);
    }

    function readMessageWithoutId() { // will attempt to read the first available message for this consumer- if it gets a 404, will attempt to read the next message
        const notifiesSql = `SELECT * from notify WHERE storeName="${node.consumerStoreName}"  and consumerId = "${node.id}" and read=false limit 1`;
        const notifies = alasql(notifiesSql);

        if (notifies.length > 0) {
            return readMessage(notifies[0].redlinkMsgId).then(msg => {
                sendMessage(msg);
            }).catch(err => {
                sendMessage({debug: err});
                readMessageWithoutId();
            })
        } 
      else 
        {
          sendMessage({failure: getFailureMessage(null, 'Store ' + node.consumerStoreName + ' does not have notifies for this consumer', 'consumerReadWithoutId')});
        }
    }

    function deleteNotify(redlinkMsgId) {
        const deleteNotifyMsg = `DELETE from notify WHERE redlinkMsgId = "${redlinkMsgId}" and storeName = "${node.consumerStoreName}" and consumerId="${node.id}"`;
        return alasql(deleteNotifyMsg);
    }

    function getMessageMetadataFromHeaders(headers) {
        const metadata = {};
        for (const property in redlinkConstants.messageFields) { //key- lowercase, value- camelCase
            if (headers[property]) { metadata[redlinkConstants.messageFields[property]] = headers[property]; }
        }
        if (Object.keys(metadata).length === 0) return undefined;
        return metadata;
    }

    function getPayload(res, redlinkMsgId) {
        return new Promise((resolve, reject) => {
            if (res instanceof stream.Stream) {
                let data = "";
                let timestamp = Date.now();
                res.on("data", chunk => data += chunk);
                res.on("end", () => { 
                   timestamp = Date.now(); 
                   const decode = base64Helper.decode(data);
                   resolve(decode);
                });
                res.on("error", error => reject(error));
            } 
          else 
            {
                resolve(base64Helper.decode(res));
            }
        });
    }

    function readMessage(redlinkMsgId) {
        return new Promise((resolve, reject) => {
            let notifiesSql;

            if (redlinkMsgId) { notifiesSql = `SELECT * from notify WHERE storeName="${node.consumerStoreName}" AND redlinkMsgId="${redlinkMsgId}" AND consumerId = "${node.id}" order by notifyTime`; } 
                         else { notifiesSql = `SELECT * from notify WHERE storeName="${node.consumerStoreName}" AND consumerId = "${node.id}" order by notifyTime`; }

            const notifies = alasql(notifiesSql);
            if (notifies.length > 0) {
                if (notifies[0].sendOnly) { //doing it here to pass an accurate enm
                    deleteNotify(redlinkMsgId);
                    if (!getNewNotify()) { sendOutNotify(); } //Get some more
                }
                const updateNotifyStatus = `UPDATE notify SET read=true WHERE redlinkMsgId="${redlinkMsgId}"  AND consumerId = "${node.id}"`;
                alasql(updateNotifyStatus);
                let notifyPathIn = base64Helper.decode(notifies[0].notifyPath);
                let notifyPath; //todo revisit this- code duplication
                // The first entry in the notify contains the enforceReversePath
                if (notifyPathIn[0].enforceReversePath) {
                    notifyPath = notifyPathIn.pop();
                } 
              else 
                {
                    notifyPath = notifyPathIn[0];
                    notifyPathIn = [];
                }
                const address = notifyPath.address + ':' + notifyPath.port;
                notifyPathIn = base64Helper.encode(notifyPathIn);
                const sendingStoreName = notifies[0].storeName;
                const readDelayCalc = Date.now() - notifies[0].notifyTime;
                // Update My Consumers Read Stats
                updateGlobalConsumerEcm(node.name, node.id, node.consumerStoreName, readDelayCalc);
                updateGlobalConsumerErm(node.name, node.id, node.consumerStoreName, readDelayCalc);
                const options = {
                    method: 'POST',
                    url: 'https://' + address + '/read-message',
                    body: {
                        redlinkMsgId,
                        notifyPath: notifyPathIn,
                        redlinkProducerId: notifies[0].redlinkProducerId,
                        consumerId: node.id,
                        consumerService: node.name,
                        consumerStoreName: node.consumerStoreName,
                        readDelay: readDelayCalc,
                        enm: calculateEnm(node.name, node.id),
                    },
                    json: true
                };
                sendMessage({debug: {"debugData": `storeName ${sendingStoreName} ${node.name}action:consumerRead${options}`}});
                request(options, function (error, response) {
                    if (response && response.statusCode === 200) {
                        const msg = getMessageMetadataFromHeaders(response.headers);
                        if (msg) {
                            let retrieveDelay;
                            if (msg.timestamp) { retrieveDelay = (Date.now() - msg.timestamp); }
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
                                enm: calculateEnm(node.name, node.id),
                                notifyReadDelay: readDelayCalc,
                                readTransport: retrieveDelay - readDelayCalc,
                                notifyCount: updateStatusCounter()
                            };
                            if (response.body) {
                                getPayload(response.body, redlinkMsgId).then(payload => {
                                    receiveMsg.payload = payload.payload ? payload.payload : payload;
                                    resolve({receive: receiveMsg});
                                    if (msg.sendOnly === 'false') {
                                        msg.sendOnly = false;
                                    }
                                    if (msg.sendOnly) { deleteNotify(redlinkMsgId); } 
                                                 else { watermark++; }
                                }).catch(err => {
                                    const errorDesc = `Error reading payload from store at :${address} ${err}`;
                                    reject({failure: getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead')});
                                    deleteNotify(redlinkMsgId);
                                });
                            }
                        } 
                      else 
                        {
                            const errorDesc = 'Message metadata not received';
                            reject({failure: getFailureMessage(redlinkMsgId, errorDesc, 'consumerRead')});
                            deleteNotify(redlinkMsgId);
                        }
                    } 
                  else 
                    {
                        // not 200- error case
                        //console.log('READ Error = ',error);
                        const failureMessage = getFailureMessage(redlinkMsgId, 'Not 200 Result', 'consumerRead');
                        failureMessage.statusCode = 404;
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


