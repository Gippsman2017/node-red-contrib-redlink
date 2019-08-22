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
    node.command = true;
    node.registration = config.showRegistration;
    node.debug = config.showDebug;

    // Insert myself into the mesh.
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    alasql(insertStoreSql);

    function getConsumersOfType() {
        const globalConsumersSql = 'SELECT DISTINCT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
        return alasql(globalConsumersSql);
    }

    function getBody(allConsumers, ips, notifyDirection) {
        return {
            consumers: allConsumers,
            notifyType: 'consumerRegistration',
            storeName: node.name,
            storeAddress: node.listenAddress,
            storePort: node.listenPort,
            ips,
            notifyDirection
        };
    }

    function sendMessage(msg) { //command, registration, debug
        const msgs = [];
        if (node.command) {
            msgs.push(msg.command);
        }
        if (node.registration) {
            msgs.push(msg.registration);
        }
        if (node.debug) {
            msgs.push(msg.debug);
        }
        node.send(msgs);
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
                direction: 'north',
                consumerId: consumer.consumerId,
                hopCount: 0  //TODO WIP
            });
        });

        const consumers = getConsumersOfType();
        const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
        if (ip && ip !== '0.0.0.0') {
            const body = getBody(allConsumers, ipTrail, notifyDirection);
            sendMessage({
                registration: {
                    storeName: node.name,
                    action: 'notifyRegistration',
                    direction: 'outBound',
                    notifyData: body
                }
            });
            const options = {
                method: 'POST',
                url: 'https://' + ip + ':' + port + '/notify',
                body,
                json: true
            };
            request(options, function (error, response) {
                if (error) {
                    sendMessage({
                        registration: {
                            storeName: node.name,
                            action: 'notifyRegistrationResult',
                            direction: 'outBound',
                            notifyData: body,
                            error: error
                        }
                    });
                } else {
                    sendMessage({
                        registration: {
                            storeName: node.name,
                            action: 'notifyRegistrationResult',
                            direction: 'outBound',
                            notifyData: response.body
                        }
                    });
                }
            });
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
                        if (error || response.statusCode !== 200) {
                            sendMessage({debug: {error: true, errorDesc: error || response.body}});
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

    function insertGlobalConsumer(consumer, thisRegistrationDirection, storeAddress, storePort, storeName) {
        const serviceName = consumer.serviceName || consumer.globalServiceName;
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + serviceName +
            '" AND direction = "' + thisRegistrationDirection +
            '" AND globalStoreIp = "' + storeAddress + '" AND globalStorePort = ' + storePort + '';
        //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        console.log('in insertGlobalConsumer... consumer is:', consumer);
        const consumerId =  consumer.consumerId || consumer.globalConsumerId;
        const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ',"' + thisRegistrationDirection + '","'+consumerId+'")';
        if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
            alasql(insertGlobalConsumersSql);
        }
    }

    app.post('/notify', (req, res) => { //todo validation on params
        const notifyType = req.body.notifyType;
        switch (notifyType) {

            case 'consumerRegistration' :
                //console.log(req.headers.host);
                sendMessage({
                    registration: {
                        storeName: node.name,
                        action: 'notifyConsumerRegistration',
                        direction: 'inBound',
                        Data: req.body
                    }
                });

                const storeName = req.body.storeName;
                const storeAddress = req.body.storeAddress;
                const storePort = req.body.storePort;
                let notifyDirection = req.body.notifyDirection;

                //register this as a south store if it is not already in list, essentially if the direction is north, then the actual store sending this consumerRegistration is south of it.
                if (notifyDirection === notifyDirections.NORTH && storeAddress && storePort) {
                    if (!node.southPeers.includes(storeAddress + ':' + storePort)) {// The registration is actually a south peer registering itself
                        //console.log('south Store Registration ',storeAddress + ':' + storePort);
                        node.southPeers.push(storeAddress + ':' + storePort);
                    }
                }
                const ips = req.body.ips;
                let thisRegistrationDirection = 'south';
                if (notifyDirection === notifyDirections.SOUTH) {
                    thisRegistrationDirection = 'north';
                }
                req.body.consumers.forEach(consumer => {
                    if (consumer.globalServiceName) { //dont add global consumer if a local consumer already exists with same name
                        //check if we have a local consumer with same name as global
                        const localConsumerSql = 'SELECT * FROM localStoreConsumers WHERE storeName="' + node.name + '" AND serviceName="' + consumer.globalServiceName + '"';
                        const localConsumersWithSameName = alasql(localConsumerSql);
                        if (localConsumersWithSameName.length === 0) {
                            insertGlobalConsumer(consumer, thisRegistrationDirection, storeAddress, storePort, storeName);
                        }
                    } else {
                        insertGlobalConsumer(consumer, thisRegistrationDirection, storeAddress, storePort, storeName);
                    }
                });
                //if the registration direction is going south, then dont notify anything north, because a south store is really not the owner and not allowed to register globals to a north store
                if (thisRegistrationDirection === 'south') {
                    notifyNorthStoreOfConsumers(ips);
                }
                notifySouthStoreOfConsumers(ips);
                const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
                const localConsumers = alasql(localConsumersSql);
                const consumers = getConsumersOfType();
                sendMessage({
                    registration: {
                        storeName: node.name,
                        toStoreName: req.body.storeName,
                        action: 'notifyConsumerRegistration',
                        direction: 'inBoundReply',
                        Data: {globalConsumers: consumers, localConsumers: localConsumers}
                    }
                });
                res.send({globalConsumers: consumers, localConsumers: localConsumers}); //TODO send back a delta- dont send back consumers just been notified of...
                break;


            case 'producerNotification' :
                sendMessage({
                    debug: {
                        storeName: node.name,
                        action: 'producerNotification',
                        direction: 'inBound',
                        Data: req.body
                    }
                });
                const existingLocalConsumerSql = 'SELECT * FROM localStoreConsumers WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '"';
                const localCons = alasql(existingLocalConsumerSql);
                if (localCons.length > 0) {
                    //If this store has a consumer on it then send out a local notify
                    //avoid inserting multiple notifies
                    const existingNotifySql = 'SELECT * FROM notify WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '" AND srcStoreIp="' + req.body.srcStoreIp + '" AND srcStorePort=' +
                        req.body.srcStorePort + ' AND redlinkMsgId="' + req.body.redlinkMsgId + '"';
                    const existingNotify = alasql(existingNotifySql);
                    console.log('existingNotifySql:', existingNotifySql,  'existingNotify:', existingNotify);
                    if (!existingNotify || existingNotify.length === 0) {
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId + '","",false,"' + req.body.redlinkProducerId + '")';
                        alasql(notifyInsertSql);
                        const allNotifies = alasql('SELECT * FROM notify');
                        sendMessage({
                            debug: {
                                storeName: node.name,
                                action: 'producerNotification',
                                direction: 'outBound',
                                Data: allNotifies
                            }
                        });
                    }
                } else {
                    // Time to Relay any peer stores north or south, the recursion is captured below
                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + req.body.service + '"';
                    const globalCons = alasql(existingGlobalConsumerSql);
                    sendMessage({
                        registration: {
                            storeName: node.name,
                            action: 'forwardProducerNotification',
                            direction: 'outBound',
                            Data: {globalCons}
                        }
                    });
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
                            sendMessage({
                                registration: {
                                    storeName: node.name,
                                    action: 'producerForwardNotification',
                                    direction: 'outBound',
                                    Data: {
                                        globalConsumers: consumer.globalStoreIp + ':' + consumer.globalStorePort,
                                        Data: body
                                    }
                                }
                            });
                            const options = {
                                method: 'POST',
                                url: 'https://' + consumer.globalStoreIp + ':' + consumer.globalStorePort + '/notify',
                                body,
                                json: true
                            };
                            // Dont forward notifies back to the sender, otherwise you end up in a recursive loop
                            if (req.body.transitPort === consumer.globalStorePort && req.body.transitIp === consumer.globalStoreIp) {
//                         console.log(node.name,' Dont forward this notify to ',consumer.globalStoreName);
                            } else {
                                request(options, function (error, response) {
                                    if (error || response.statusCode !== 200) {
                                        sendMessage({debug: {error: true, errorDesc: error || response.body}});
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
        sendMessage({debug: {storeName: node.name, action: 'read-message', direction: 'inBound', Data: req.body}});
        if (!redlinkMsgId) {
            sendMessage({
                debug: {
                    storeName: node.name,
                    action: 'read-message',
                    direction: 'outBound',
                    error: 'redlinkMsgId not specified-400'
                }
            });
            res.status(400).send({err: 'redlinkMsgId not specified'});
            return;
        }
        const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + redlinkMsgId + '" AND read=' + false;
        const msgs = alasql(msgSql);//should get one or none
        if (msgs.length > 0) { //will be zero if the message has already been read
            sendMessage({
                debug: {
                    storeName: node.name,
                    action: 'read-message',
                    direction: 'outBound',
                    Data: msgs[msgs.length - 1],
                    error: 'none'
                }
            });
            res.send(msgs[0]); //send the oldest message first
            //update message to read=true
            const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + msgs[0].redlinkMsgId + '"';
            alasql(updateMsgStatus);
        } else {
            sendMessage({
                debug: {
                    storeName: node.name,
                    action: 'read-message',
                    direction: 'outBound',
                    error: 'No unread messages'
                }
            });
            const msg = redlinkMsgId ? 'Message with id ' + redlinkMsgId + ' not found' : 'No unread messages';
            res.status(404).send({error: true, msg});
        }
    });

    app.post('/reply-message', (req, res) => {
        const redlinkMsgId = req.body.redlinkMsgId;
        const redlinkProducerId = req.body.redlinkProducerId;
        // const replyingService = req.body.replyingService;
        const message = req.body.payload;
        sendMessage({
            debug: {
                storeName: node.name, action: 'reply-message', direction: 'inBound', Data: req.body
            }
        });
        const replyMsgSql = 'SELECT DISTINCT * FROM replyMessages WHERE redlinkMsgId="' + redlinkMsgId + '"';
        const replyMsg = alasql(replyMsgSql);
        if (replyMsg.length === 0) {
            const insertReplySql = 'INSERT INTO replyMessages ("' + node.name + '","' + redlinkMsgId + '","' + redlinkProducerId + '","' + message + '", false)';
            alasql(insertReplySql);
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
        switch (msg.topic) {
            case 'listRegistrations' : {
                sendMessage({command: getAllVisibleConsumers()});
                break;
            }
            case 'listStore'         : {
                sendMessage({command: getCurrentStoreData()});
                break;
            }
            case 'flushStore'        : {
                const removeReplySql = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
                const removeNotifySql = 'DELETE FROM notify        WHERE storeName="' + node.name + '"';
                const removeInMessagesSql = 'DELETE FROM inMessages    WHERE storeName="' + node.name + '"';
                alasql(removeReplySql);
                alasql(removeNotifySql);
                alasql(removeInMessagesSql);
                sendMessage({command: getCurrentStoreData()});
                break;
            }
            default                     : {
                sendMessage({command: {help: "msg.topic can be listRegistrations listStore flushStore"}});
                break;
            }
        }
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

