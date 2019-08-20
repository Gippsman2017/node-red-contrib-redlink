const httpsServer = require('./https-server.js');
const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkStore = function (config) {
    const notifyDirections = {
        NORTH: 'north',
        SOUTH: 'south'
    };

    RED.nodes.createNode(this, config);
    const node = this;
    const log = require('./log.js')(node).log;

    node.listenAddress = config.listenAddress;
    node.listenPort = config.listenPort;
    node.meshName = config.meshName;
    node.name = config.meshName ? config.meshName + ':' + config.name : config.name;
    node.notifyInterval = config.notifyInterval;
    node.functions = config.functions;
    node.northPeers = config.headers; //todo validation in ui to prevent multiple norths with same ip:port
    node.southPeers = []; //todo each store should notify its north peer once when it comes up- that's how southPeers will be populated

    // Insert myself into the mesh.
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    alasql(insertStoreSql);

    function getConsumersOfType() {
        const globalConsumersSql = 'SELECT DISTINCT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
        return alasql(globalConsumersSql);
    }

    function getBody(allConsumers, ipTrail, notifyDirection) {
        return {
            consumers: allConsumers,
            notifyType: 'consumerRegistration',
            storeName: node.name,
            storeAddress: node.listenAddress,
            storePort: node.listenPort,
            ips: ipTrail,
            notifyDirection
        };
    }

    function notifyPeerStoreOfConsumers(ip, port, ipTrail, notifyDirection) {
        if (!ipTrail) {
            ipTrail = [];
        }  //loop in notifications- dont send it
        if (ipTrail.includes(ip + ':' + port)) {
            return;
        } else {
            ipTrail.push(ip + ':' + port);
        }
        // first get distinct local consumers
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const localConsumers = alasql(localConsumersSql);
        let qualifiedLocalconsumers = [];
        localConsumers.forEach(consumer => {
            qualifiedLocalconsumers.push({
                localStoreName: consumer.storeName,
                globalStoreName: consumer.localStoreName,
                globalServiceName: consumer.serviceName,
                globalStoreIp: node.listenAddress,
                globalStorePort: node.listenPort,
                direction: 'north'
            });
        });

        const consumers = getConsumersOfType();
        const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
        if (ip && ip !== '0.0.0.0') {
            const body = getBody(allConsumers, ipTrail, notifyDirection);
            node.send([null, {
                storeName: node.name,
                action: 'notifyRegistration',
                direction: 'outBound',
                notifyData: body
            }, null]);
            const options = {
                method: 'POST',
                url: 'https://' + ip + ':' + port + '/notify',
                body,
                json: true
            };
            request(options, function (error, response) {

                if (error) {
                    node.send([null, {
                        storeName: node.name,
                        action: 'notifyRegistrationResult',
                        direction: 'outBound',
                        notifyData: body,
                        error: error
                    }, null]);
                } else {
                    node.send([null, {
                        storeName: node.name,
                        action: 'notifyRegistrationResult',
                        direction: 'outBound',
                        notifyData: response.body
                    }, null]);
                }
                // This is the return message from the post, it contains the north peer consumers, so, we will update our services.
                // node.send({store:node.name,ip:ip,port:port,ipTrail:ipTrail,consumers:body});
            });
        } else {
        }
    }

    function notifyNorthStoreOfConsumers(northIps) {
        node.northPeers.forEach(peer => {
            notifyPeerStoreOfConsumers(peer.ip, peer.port, northIps, notifyDirections.NORTH);
        });
    }

    function notifySouthStoreOfConsumers(southIps) {
        node.southPeers.forEach(peer => {
            var [ip, port] = peer.split(':');
            notifyPeerStoreOfConsumers(ip, port, southIps, notifyDirections.SOUTH);
        });
    }

    const nodeId = config.id.replace('.', '');
    const newMsgTriggerName = 'onNewMessage' + nodeId;
    const registerConsumerTriggerName = 'registerConsumer' + nodeId;

    function getRemoteMatchingStores(serviceName, meshName) {
        const globalStoresSql = 'SELECT * FROM globalStoreConsumers WHERE globalServiceName="' + serviceName + '" AND localStoreName = "' + node.name + '"';
        const matchingGlobalStores = alasql(globalStoresSql);
        const matchingGlobalStoresAddresses = [];
        matchingGlobalStores.forEach(store => {
            matchingGlobalStoresAddresses.push(store.globalStoreIp + ':' + store.globalStorePort);
        });
        return matchingGlobalStoresAddresses;
    }

    //---------------------------------------------------  Notify Triggers  ---------------------------------------------------------
    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '" AND read=' + false;
            var newMessages = alasql(newMessagesSql);
            const newMessage = newMessages[newMessages.length - 1];
            if (newMessage) {
                const allVisibleConsumers = getAllVisibleConsumers(); //todo optimise this
                //insert one notify for local
                for (const localConsumer of allVisibleConsumers.localConsumers) {
                    if (localConsumer.serviceName === newMessage.serviceName) {
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessage.serviceName + '","' + node.listenAddress + '",' + node.listenPort + ',"' + newMessage.redlinkMsgId + '","",false,"' + newMessage.redlinkProducerId + '")';
                        alasql(notifyInsertSql);
                        break; //should get only one local consumer with the same name- this is a just in case
                    }
                }
                //one for each store having a matching global consumer
                const remoteStores = new Set();
                allVisibleConsumers.globalConsumers.forEach(globalConsumer => {
                    if (globalConsumer.globalServiceName === newMessage.serviceName) {
                        remoteStores.add(globalConsumer.globalStoreName);
                    }
                });

                //This notify only handles LOCAL consumers, the /notify listener will do the forwarding
                //stores table contains stores local to this node-red instance, all consumers will contain consumers on stores reachable from this store- even if they are remote, however they are handled by the listener.

                const remoteMatchingStores = getRemoteMatchingStores(newMessage.serviceName, node.meshName);
                remoteMatchingStores.forEach(remoteStore => {
                    const body = {
                        service: newMessage.serviceName,
                        srcStoreIp: node.listenAddress,
                        srcStorePort: node.listenPort,
                        transitIp: node.listenAddress,   // Used by producerForwarderNotify
                        transitPort: node.listenPort,      // Used by producerForwarderNotify
                        redlinkMsgId: newMessage.redlinkMsgId,
                        notifyType: 'producerNotification',
                        redlinkProducerId: newMessage.redlinkProducerId
                    };
                    const options = {
                        method: 'POST',
                        url: 'https://' + remoteStore + '/notify',
                        body,
                        json: true
                    };
                    request(options, function (error, response) {
                        if (error) {
                        } else { // Should really do something with the response .. John
                            //console.log('response=',response.body);
                        }
                    });
                });
            }
        };
        //On local consumer registration, let them all know
        alasql.fn[registerConsumerTriggerName] = () => {
            notifyNorthStoreOfConsumers([]);
        };

        const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName + ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
        const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName + ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
        try {
            alasql(createNewMsgTriggerSql);
            alasql(createRegisterConsumerSql);
            notifyNorthStoreOfConsumers([]);
            notifySouthStoreOfConsumers([]);
        } catch (e1) {
        }
    } catch (e) {
        log(e);
    }

    if (node.listenPort) {
        try {
            node.listenServer = httpsServer.startServer(+node.listenPort);
        } catch (e) {
            console.log('error starting listen server on ', node.listenPort, e);
        }
        if (node.listenServer) {
            node.on('close', (removed, done) => {
                node.listenServer.close(() => {
                    done();
                });
            })
        }
        log('started server at port:', node.listenPort);
    }

    const app = httpsServer.getExpressApp();

    app.post('/notify', (req, res) => { //todo validation on params
        const notifyType = req.body.notifyType;
        switch (notifyType) {

            case 'consumerRegistration' :
                //console.log(req.headers.host);
                node.send([null, {
                    storeName: node.name,
                    action: 'notifyConsumerRegistration',
                    direction: 'inBound',
                    Data: req.body
                }, null]);

                const storeName = req.body.storeName;
                const storeAddress = req.body.storeAddress;
                const storePort = req.body.storePort;
                let notifyDirection = req.body.notifyDirection;

                //register this as a south store if it is not already in list, essentially if the direction is north, then the actual store sending this consumerRegistration is south of it.
                if (notifyDirection === notifyDirections.NORTH && storeAddress && storePort) {
                    if (!node.southPeers.includes(storeAddress + ':' + storePort)) {// The registration is actually a south peer registering itself
                        //console.log('south Store Registration ',storeAddress + ':' + storePort);
                        node.southPeers.push(storeAddress + ':' + storePort);
                    } else {
                        // console.log('north Store Registration ',storeAddress + ':' + storePort,' already registered');
                    }
                }

                const ips = req.body.ips;
                //log('the ips trail is:', ips);

                let thisRegistrationDirection = 'south';
                if (notifyDirection === notifyDirections.SOUTH) {
                    thisRegistrationDirection = 'north';
                }
                ;
                //console.log('REQUEST = ',req.body);                
                req.body.consumers.forEach(consumer => {
                    const serviceName = consumer.serviceName || consumer.globalServiceName;

                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + serviceName +
                        '" AND direction = "' + thisRegistrationDirection +
                        '" AND globalStoreIp = "' + storeAddress + '" AND globalStorePort = ' + storePort + '';
                    //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
                    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                    //console.log(existingGlobalConsumer,'-', existingGlobalConsumerSql );                    
                    const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ',"' + thisRegistrationDirection + '")';
//                    console.log(node.name,'  Wants  - ',insertGlobalConsumersSql);
                    if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
//                       console.log(node.name,' - ',insertGlobalConsumersSql);
                        alasql(insertGlobalConsumersSql);
                    } else {
                    }
                });

                //if the registration direction is going south, then dont notify anything north, because a south store is really not the owner and not allowed to register globals to a north store
                if (thisRegistrationDirection == 'south') {
                    notifyNorthStoreOfConsumers(ips);
                }
                notifySouthStoreOfConsumers(ips);

                const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
                const localConsumers = alasql(localConsumersSql);
                const consumers = getConsumersOfType();
                node.send([null, {
                    storeName: node.name,
                    toStoreName: req.body.storeName,
                    action: 'notifyConsumerRegistration',
                    direction: 'inBoundReply',
                    Data: {globalConsumers: consumers, localConsumers: localConsumers}
                }, null]);
                res.send({globalConsumers: consumers, localConsumers: localConsumers}); //TODO send back a delta- dont send back consumers just been notified of...
                break;


            case 'producerNotification' :

                node.send([null, null, {
                    storeName: node.name,
                    action: 'producerNotification',
                    direction: 'inBound',
                    Data: req.body
                }]);
                const existingLocalConsumerSql = 'SELECT * FROM localStoreConsumers WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '"';
                const localCons = alasql(existingLocalConsumerSql);
                if (localCons.length > 0) {
                    //If this store has a consumer on it then send out a local notify
                    //avoid inserting multiple notifies
                    const existingNotify = alasql('SELECT * FROM notify WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '" AND srcStoreIp="' + req.body.srcStoreIp + '" AND srcStorePort=' +
                        req.body.srcStorePort + ' AND redlinkMsgId="' + req.body.redlinkMsgId + '"');
                    if (!existingNotify || existingNotify.length === 0) {
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId + '","",false,"' + req.body.redlinkProducerId + '")';
                        //(storeName STRING, serviceName STRING, srcStoreIp STRING, srcStorePort INT , redlinkMsgId STRING, notifySent STRING)
                        alasql(notifyInsertSql);

                        // console.log('INSERT LOCAL NOTIFY ',node.name);
                        const allNotifies = alasql('SELECT * FROM notify');
                        node.send([null, null, {
                            storeName: node.name,
                            action: 'producerNotification',
                            direction: 'outBound',
                            Data: allNotifies
                        }]);
                    }
                } else {
                    // Time to Relay any peer stores north or south, the recursion is captured below

                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + req.body.service + '"';
                    const globalCons = alasql(existingGlobalConsumerSql);

                    node.send([null, {
                        storeName: node.name,
                        action: 'forwardProducerNotification',
                        direction: 'outBound',
                        Data: {globalCons}
                    }, null]);

                    if (globalCons.length > 0) {
                        globalCons.forEach(consumer => {

                            //If this store has a consumer on it then send out a peer notify    //avoid inserting multiple notifies
                            const body = {
                                service: req.body.service,
                                srcStoreIp: req.body.srcStoreIp,
                                srcStorePort: req.body.srcStorePort,
                                transitIp: node.listenAddress,   // Used by producerForwarderNotify
                                transitPort: node.listenPort,      // Used by producerForwarderNotify
                                redlinkMsgId: req.body.redlinkMsgId,
                                notifyType: 'producerNotification',
                                redlinkProducerId: req.body.redlinkProducerId
                            };

//                       console.log('Sending producerForwardNotification from ',node.name,'   to  ', consumer.globalStoreIp+':'+consumer.globalStorePort);

//                       console.log(node.name,'      forward this notify to ',consumer.globalStoreName);

                            node.send([null, {
                                storeName: node.name,
                                action: 'producerForwardNotification',
                                direction: 'outBound',
                                Data: {
                                    globalConsumers: consumer.globalStoreIp + ':' + consumer.globalStorePort,
                                    Data: body
                                }
                            }, null]);
                            const options = {
                                method: 'POST',
                                url: 'https://' + consumer.globalStoreIp + ':' + consumer.globalStorePort + '/notify',
                                body,
                                json: true
                            };

                            //console.log(node.listenPort,'  -  ',req.body.transitPort,'  -  ',consumer.globalStorePort);

                            // Dont forward notifies back to the sender, otherwise you end up in a recursive loop
                            if (req.body.transitPort == consumer.globalStorePort && req.body.transitIp == consumer.globalStoreIp) {
//                         console.log(node.name,' Dont forward this notify to ',consumer.globalStoreName);
                            } else {

                                request(options, function (error, response) {
                                    if (error) {
                                    } else {
                                        //console.log('response=',response.body);
                                    }
                                });
                            } //else transit check
                        });
                    } //else
                }

                res.status(200).send({action: 'producerNotification', status: 200});
                break;
        } //case
    }); // notify

    app.post('/read-message', (req, res) => {
        const redlinkMsgId = req.body.redlinkMsgId;
        node.send([null, null, {storeName: node.name, action: 'read-message', direction: 'inBound', Data: req.body}]);
        if (!redlinkMsgId) {
            node.send([null, null, {
                storeName: node.name,
                action: 'read-message',
                direction: 'outBound',
                error: 'redlinkMsgId not specified-400'
            }]);
            res.status(400).send({err: 'redlinkMsgId not specified'});
            return;
        }
        const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + redlinkMsgId + '" AND read=' + false;
        const msgs = alasql(msgSql);//should get one or none
        if (msgs.length > 0) { //will be zero if the message has already been read
            node.send([null, null, {
                storeName: node.name,
                action: 'read-message',
                direction: 'outBound',
                Data: msgs[msgs.length - 1],
                error: 'none'
            }]);
            res.send(msgs[0]); //send the oldest message first
            //update message to read=true
            const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + msgs[0].redlinkMsgId + '"';
            alasql(updateMsgStatus);
        } else {
            node.send([null, null, {
                storeName: node.name,
                action: 'read-message',
                direction: 'outBound',
                error: 'No unread messages'
            }]);
            const msg = redlinkMsgId ? 'Message with id ' + redlinkMsgId + ' not found' : 'No unread messages';
            res.status(404).send({error: true, msg});
        }
    });

    app.post('/reply-message', (req, res) => {
        const redlinkMsgId = req.body.redlinkMsgId;
        const redlinkProducerId = req.body.redlinkProducerId;
        // const replyingService = req.body.replyingService;
        const message = req.body.payload;
        const host = req.headers.host;//store address of replying store
        node.send([null, null, {storeName: node.name, action: 'reply-message', direction: 'inBound', Data: req.body}]);
        const replyMsgSql = 'SELECT DISTINCT * FROM replyMessages WHERE redlinkMsgId="' + redlinkMsgId + '"';
        const replyMsg = alasql(replyMsgSql);
        if (replyMsg.length == 0) {
            const insertReplySql = 'INSERT INTO replyMessages ("' + node.name + '","' + redlinkMsgId + '","' + redlinkProducerId + '","' + message + '", false)';
            const inserteply = alasql(insertReplySql);
            res.status(200).send({msg: 'Reply received for ', redlinkMsgId: redlinkMsgId});
        } else {
            res.status(404).send({msg: 'Reply Rejected for ', redlinkMsgId: redlinkMsgId});
        }
    });


    function getAllVisibleConsumers() {
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalStoreName<>localStoreName';
        const storesSql = 'SELECT * FROM stores where storeName ="' + node.name + '"';
        const localConsumers = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        const store = alasql(storesSql);
        const peers = {"northPeers": node.northPeers, "southPeers": node.southPeers};
        return {
            localConsumers,
            globalConsumers,
            store,
            peers
        };
    }

    function getCurrentStoreData() {
        const messagesSql = 'SELECT * FROM inMessages    where storeName ="' + node.name + '"';
        const notifiesSql = 'SELECT * FROM notify        where storeName ="' + node.name + '"';
        const repliesSql = 'SELECT * FROM replyMessages where storeName ="' + node.name + '"';
        const storeSql = 'SELECT * FROM stores        where storeName ="' + node.name + '"';
        const messages = alasql(messagesSql);
        const notifies = alasql(notifiesSql);
        const replies = alasql(repliesSql);
        const store = alasql(storeSql);
        return {
            store,
            messages,
            notifies,
            replies
        };
    }

    node.on("input", msg => {
        log(msg);
        switch (msg.payload) {
            case 'listRegistrations' : {
                const allConsumers = getAllVisibleConsumers();
                node.send(allConsumers);
                break;
            }
            case 'listStore'         : {
                const storeData = getCurrentStoreData();
                node.send(storeData);
                break;
            }
            case 'flushStore'        : {
                const removeReplySql = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
                const removeNotifySql = 'DELETE FROM notify        WHERE storeName="' + node.name + '"';
                const removeInMessagesSql = 'DELETE FROM inMessages    WHERE storeName="' + node.name + '"';
                alasql(removeReplySql);
                alasql(removeNotifySql);
                alasql(removeInMessagesSql);
                const storeData = getCurrentStoreData();
                node.send(storeData);
                break;
            }
            default                     : {
                node.send({help: "msg.payload can be listRegistrations listStore flushStore"});
                break;
            }
        }
        //todo what messages should we allow? register and notify are handled via endpoints
    });

    node.on('close', (removed, done) => {
        const removeReplySql = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
        const removeNotifySql = 'DELETE FROM notify        WHERE storeName="' + node.name + '"';
        const removeInMessagesSql = 'DELETE FROM inMessages    WHERE storeName="' + node.name + '"';
        alasql(removeReplySql);
        alasql(removeNotifySql);
        alasql(removeInMessagesSql);

        const removeStoreSql = 'DELETE FROM stores WHERE storeName="' + node.name + '"';
        const removeDirectConsumersSql = 'DELETE FROM localStoreConsumers  WHERE storeName="' + node.name + '"';
        const removeGlobalConsumersSql = 'DELETE FROM globalStoreConsumers WHERE globalStoreName="' + node.name + '"';
        alasql(removeStoreSql);
        alasql(removeDirectConsumersSql);
        alasql(removeGlobalConsumersSql);
        //also delete all associated consumers for this store name
        // const dropTriggerNewMsg = 'DROP TRIGGER ' + newMsgTriggerName;
        // alasql(dropTriggerNewMsg);
        dropTrigger(newMsgTriggerName);
        // const dropTriggerRegisterConsumer = 'DROP TRIGGER ' + registerConsumerTriggerName;
        // alasql(dropTriggerRegisterConsumer);
        dropTrigger(registerConsumerTriggerName);
        done();
    });

    function dropTrigger(triggerName) { //workaround for https://github.com/agershun/alasql/issues/1113
        alasql.fn[triggerName] = () => {
            // console.log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
        }
    }
}; // function

