
const alasql         = require('alasql');
const fs             = require('fs-extra');
const pick           = require('lodash.pick');
const Readable       = require('stream').Readable;
const request        = require('request').defaults({strictSSL: false});
const requestPromise = require('request-promise-native').defaults({strictSSL: false});

const base64Helper   = require('./base64-helper.js');
const httpsServer    = require('./https-server.js');
const {messageFields, notifyDirections} = require('./redlinkConstants');
const {calculateEnm, closeStore, insertGlobalConsumer, sendMessage, updateGlobalConsumerEcm, updateGlobalConsumerEnm, updateGlobalConsumerErm} = require('./store-common');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkStore = function (config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.reSyncTime = 30000; // This timer defines the routing mesh sync for any messages.
    node.consumerlifeSpan = 120; // 2 Minutes
    node.reSyncTimerId = {};
    node.statusTime = 4000; // This timer defines the routing mesh sync for any messages.
    node.statusTimerId = {};
    node.listenAddress = config.listenAddress;
    node.listenPort = config.listenPort;
    node.southInsert = config.southInsert;
    node.meshName = config.meshName;
    node.name = config.meshName ? config.meshName + ':' + config.name : config.name;
    node.notifyInterval = config.notifyInterval;
    node.functions = config.functions;
    node.northPeers = config.headers; // todo validation in ui to prevent multiple norths with same ip:port
    node.southPeers = []; // todo each store should notify its north peer once when it comes up- that's how southPeers will be populated
    node.command = true;
    node.registration = config.showRegistration;
    node.debug = config.showDebug;
    node.isUserCertificate = config.isUserCertificate;
    node.userKey = config.userKey;
    node.userCertificate = config.userCertificate;
    node.interStoreLoadBalancer = config.interStoreLB;
    require('./redlinkSettings.js')(RED, node.name).cleanLargeMessagesDirectory();
    node.largeMessagesDirectory = require('./redlinkSettings.js')(RED, node.name).largeMessagesDirectory;
    const largeMessageThreshold = require('./redlinkSettings.js')(RED, node.name).largeMessageThreshold;

    // Insert myself into the mesh.
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    alasql(insertStoreSql);

    function notifyPeerStoreOfLocalConsumers(address, port, transitAddress, transitPort) {
        // first get distinct local consumers
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const localConsumers = alasql(localConsumersSql);
        localConsumers.forEach(consumerS => {
            let qualifiedLocalConsumers = [];
            qualifiedLocalConsumers.push({
                localStoreName: node.name,
                storeName: consumerS.storeName,
                serviceName: consumerS.serviceName,
                consumerId: consumerS.consumerId,
                storeAddress: address,
                storePort: port,
                transitAddress: transitAddress,
                transitPort: transitPort,
                direction: 'local',
                hopCount: 0,
                ttl: Math.floor(new Date().getTime() / 1000) + node.consumerlifeSpan // Setup the overall lifetime for this service, its epoch secs + overall lifespan in secs
            });

            let consumer = qualifiedLocalConsumers[0];
            let body = {consumer, notifyType: 'consumerRegistration'};
            if (address && address !== '0.0.0.0') {
                sendMessage({ registration: { storeName: node.name, action: 'notifyRegistration', function: 'notifyPeerStoreOfLocalConsumers', notifyData: body } }, node);
                sendMessage({ debug: { storeName: node.name, action: 'notifyRegistration', direction: 'outBound', notifyData: body } }, node);
                const options = {method: 'POST', url: 'https://' + address + ':' + port + '/notify', body, json: true};
                request(options, function (error, response) {
                    if (error) { sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: error } }, node); } 
                          else { sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: response.body } }, node); }
                });
            }
        });
    }

    function deleteSouthPeer(address, port, peer) {
        return peer.filter(function (addressPort) {
            return addressPort !== address + ':' + port;
        });
    }

    function deleteGlobalStoreConsumers(store, direction, address, port) {
        const deleteSql = 'delete from globalStoreConsumers where localStoreName="' + store + '" and direction="' + direction + '" and transitAddress="' + address + '" and transitPort=' + port;
        const deleteResult = alasql(deleteSql);
        return (deleteResult > 0);
    }

    function notifyPeerStoreOfConsumers(consumer, direction, hopCount, address, port, transitAddress, transitPort) {
        consumer.direction = direction;
        consumer.transitAddress = transitAddress;
        consumer.transitPort = transitPort;
        consumer.hopCount = hopCount;
        let body = {consumer, notifyType: 'consumerRegistration'};
        if (address && address !== '0.0.0.0') {
            sendMessage({ debug: { storeName: node.name, action: 'notifyRegistration', direction: 'outBound', notifyData: body } }, node);
            const options = {method: 'POST', url: 'https://' + address + ':' + port + '/notify', body, json: true};
            request(options, function (error, response) {
                if (error) {
                    if (direction === 'south') {
                        node.southPeers = deleteSouthPeer(address, port, node.southPeers); // Ok, looks like the peer has gone, so, lets delete the peer entry.
                        deleteGlobalStoreConsumers(node.name, direction, address, port);
                        sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConnection', direction: direction, notifyData: address + ':' + port, serviceName: consumer.serviceName } }, node);
                    } 
                  else 
                    if (direction === 'north') { // Ok, North Peers are hard wired, if the connection is a problem, then just delete the globalStoreConsumers.
                        if (deleteGlobalStoreConsumers(node.name, direction, address, port)) {
                            sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConsumer', direction: direction, notifyData: address + ':' + port, serviceName: consumer.serviceName } }, node);
                        }
                    }
                    sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: error } }, node);    
                } 
              else 
                if (direction === 'south' && response.statusCode === 404) {
                    node.southPeers = deleteSouthPeer(address, port, node.southPeers); // Ok, looks like the peer has rejected the update, so, lets delete the peer entry.
                    deleteGlobalStoreConsumers(node.name, direction, address, port);
                    sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConnection', direction: direction, notifyData: address + ':' + port, serviceName: consumer.serviceName } }, node);
                } 
              else 
                {
                    sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: response.body } }, node);
                }
            });
        }
    }

    function notifyAllNorthPeerStoresOnly() {
        const consumer = {
            direction: 'store',
            transitAddress: node.listenAddress,
            transitPort: node.listenPort
        };
        node.northPeers.forEach(peer => { notifyPeerStoreOfConsumers(consumer, consumer.direction, 0, peer.ip, peer.port, consumer.transitAddress, consumer.transitPort); });
    }


    function notifyNorthStoreOfConsumers(consumer, transitAddress, transitPort) {
        const newHopCount = consumer.hopCount + 1;
        node.northPeers.forEach(peer => {
            if (consumer.transitAddress + ':' + consumer.transitPort !== peer.ip + ':' + peer.port) {
                notifyPeerStoreOfConsumers(consumer, notifyDirections.NORTH, newHopCount, peer.ip, peer.port, transitAddress, transitPort);
            }
        });
    }

    function notifySouthStoreOfConsumers(consumer, direction, storeAddress, storePort, transitAddress, transitPort) {
        const newHopCount = consumer.hopCount + 1;
        node.southPeers.forEach(peer => {
            var [ip, port] = peer.split(':');
            if (consumer.storeAddress + ':' + consumer.storePort !== ip + ':' + port) {
                notifyPeerStoreOfConsumers(consumer, direction, newHopCount, ip, port, transitAddress, transitPort);
            }
        });
    }

    function notifyAllSouthStoreConsumers(direction) {
        const storeName = node.name;
        const storeAddressData = alasql(`SELECT * FROM stores  WHERE storeName = "${storeName}"`);
        const globalConsumersSql = `SELECT  DISTINCT serviceName,consumerId from ( select * from globalStoreConsumers WHERE localStoreName = "${storeName}")`;// +
        const globalConsumers = alasql(globalConsumersSql);
        const allConsumers = [...new Set([...globalConsumers])];
        const storeAddress = storeAddressData[0].storeAddress;
        const storePort = storeAddressData[0].storePort;
        allConsumers.forEach(consumer => {
            const dataSql = `select * from globalStoreConsumers where localStoreName = "${storeName}" and serviceName ="${consumer.serviceName}" and consumerId = "${consumer.consumerId}"`;
            const data = alasql(dataSql);
            notifySouthStoreOfConsumers(data[0], direction, storeAddress, storePort, node.listenAddress, node.listenPort);
        });
    }

    const nodeId = config.id.replace('.', '');
    node.newMsgTriggerName = `onNewMessage${nodeId}`;
    node.registerConsumerTriggerName = `registerConsumer${nodeId}`;

    function getRemoteMatchingStores(serviceName, meshName, loadBalancer, useEnm) {
        let matchingGlobalStoresAddresses = [];
        let globalStoresSql = `SELECT * FROM globalStoreConsumers WHERE serviceName="${serviceName}" AND localStoreName = "${node.name}"  order by enm  limit 1`;
        if (loadBalancer) {
            const matchingGlobalStores = alasql(globalStoresSql);
            if (matchingGlobalStores.length > 0) {
                matchingGlobalStoresAddresses.push({
                    transitStoreAddress: matchingGlobalStores[0].transitAddress + ':' + matchingGlobalStores[0].transitPort,
                    transitHopCount: matchingGlobalStores[0].hopCount,
                    transitEcm: matchingGlobalStores[0].ecm,
                    transitErm: matchingGlobalStores[0].erm,
                    transitEnm: matchingGlobalStores[0].enm
                });
            } else
                matchingGlobalStoresAddresses = [];
        } else { // No Load Balancer on this store
            const matchingGlobalStores = alasql(globalStoresSql);
            matchingGlobalStores.forEach(store => {
                matchingGlobalStoresAddresses.push({
                    transitStoreAddress: store.transitAddress + ':' + store.transitPort,
                    transitHopCount: store.hopCount,
                    transitEcm: store.ecm,
                    transitErm: store.erm,
                    transitEnm: store.enm
                });
            });
        }
        return matchingGlobalStoresAddresses;
    }


    function notifyUnreadMessages() {
        // check if the input message is for this store
        // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
        const newMessages = alasql(`SELECT * from inMessages WHERE storeName="${node.name}" AND read= false AND consumerId = ""  ORDER BY priority DESC limit 1`);
        if (newMessages.length > 0) {
            const newMessage = newMessages[0];
            if (newMessage) {
                sendMessage({ registration: { service: newMessage.serviceName, srcStoreAddress: node.listenAddress, srcStorePort: node.listenPort, redlinkMsgId: newMessage.redlinkMsgId, action: 'producerNotification', redlinkProducerId: newMessage.redlinkProducerId } }, node);
                const updateMsgStatus = `UPDATE inMessages SET consumerId ="pending"  WHERE consumerId = "" AND redlinkMsgId="${newMessage.redlinkMsgId}" AND storeName = "${node.name}"`;
                alasql(updateMsgStatus);
                let producerStores = [...new Set([...getRemoteMatchingStores(newMessage.serviceName, node.meshName, node.interStoreLoadBalancer, newMessage.enforceReversePath)])];
                let thisPath = []; // Effectively sets the start path to this store, as the notify adds it in the /notify code
                producerStores.forEach(remoteStore => {
                    let body = {
                        service: newMessage.serviceName,
                        srcStoreAddress: node.listenAddress,
                        srcStorePort: node.listenPort,
                        srcStoreName: node.name,
                        transitAddress: node.listenAddress,
                        transitPort: node.listenPort,
                        notifyPath: thisPath,
                        sendersHopCount: remoteStore.transitHopCount,
                        redlinkMsgId: newMessage.redlinkMsgId,
                        notifyType: 'producerNotification',
                        redlinkProducerId: newMessage.redlinkProducerId,
                        enforceReversePath: newMessage.enforceReversePath,
                        consumerId: newMessage.consumerId
                    };
                    const options = { method: 'POST', url: 'https://' + node.listenAddress + ':' + node.listenPort + '/notify', body, json: true };
                    // Ok Ask the Consumer if they have the capacity to consume this message
                    (async function () {
                        let response;
                        try { response = await requestPromise(options); } 
                        catch (e) { sendMessage({ debug: { storeName: node.name, action: `POST to ${options.url} `, direction: 'outBound', error: e } }, node); }
                        if (!response) { return {}; }

                        // Ok, if a consumerId is not empty, then set the message with the consumerId and use the returned path to go directly to the consumer store.
                        let updateMsgStatus = `UPDATE inMessages SET consumerId =""  WHERE consumerId = "pending" AND redlinkMsgId="${body.redlinkMsgId}" AND storeName = "${node.name}"`;  // Not OK, no capacity at this time
                        if (response.consumerId !== '') { updateMsgStatus = `UPDATE inMessages SET consumerId ="${response.consumerId}"  WHERE consumerId = "pending" AND redlinkMsgId="${body.redlinkMsgId}" AND storeName = "${node.name}"`; } 
                        alasql(updateMsgStatus);

                        // Ok, if a consumerId is not empty, then use the returned path to go directly to the consumer store and confirm the message, this will cause the consumer store to notify the consumerId
                        if (response.consumerId !== '') {
                            let confirmBody = body;
                            confirmBody.notifyPath = response.notifyPath;
                            confirmBody.consumerId = response.consumerId;
                            confirmBody.notifyType = 'confirmNotification';
                            confirmBody.tempPath = response.notifyPath; // This is used to traverse to the consumer store again, and it is removed once it gets there.
                            body = confirmBody;
                            const options = { method: 'POST', url: 'https://' + node.listenAddress + ':' + node.listenPort + '/notify', body, json: true };
                            //  Recurse to this remoteStore
                            (async function () {
                                let response2;
                                try { response2 = await requestPromise(options); } 
                                catch (e) { sendMessage({ debug: { storeName: node.name, action: `POST to ${options.url}`, direction: 'outBound', error: e } }, node); }
                                if (response2 && response2.consumerId !== '') { updateGlobalConsumerEnm(response2.service, response2.consumerId, response2.storeName, response2.enm, node.name); } // OK, I have a winner
                                return response2;
                            })();
                        }
                        return response;
                    })(); // This is the actual call to get the notify going, it causes recursive /producerNotifies lookups through the stores.
                });
            }
        }
    }

    //---------------------------------------------------  Notify Triggers  ---------------------------------------------------------
    try {
        alasql.fn[node.newMsgTriggerName] = () => {
          notifyUnreadMessages();
        };

        // On local consumer registration, let them all know
        alasql.fn[node.registerConsumerTriggerName] = () => {
            // Notify my local globalConsumerStore with a default hop count of zero and a transit address of myself...all the localConsumers.
            notifyPeerStoreOfLocalConsumers(node.listenAddress, node.listenPort, node.listenAddress, node.listenPort);
        };

        const createNewMsgTriggerSql    = `CREATE TRIGGER ${node.newMsgTriggerName} AFTER INSERT ON inMessages CALL ${node.newMsgTriggerName}()`;
        const createRegisterConsumerSql = `CREATE TRIGGER ${node.registerConsumerTriggerName} AFTER INSERT ON localStoreConsumers CALL ${node.registerConsumerTriggerName}()`;
        try {
            alasql(createNewMsgTriggerSql);
            alasql(createRegisterConsumerSql);
        } 
        catch (e1) {
            sendMessage({ debug: e1 }, node);
        }
        notifyAllNorthPeerStoresOnly(); // Register myself with my North Store
        node.reSyncTimerId = reSyncStores(node.reSyncTime); // This is the main call to sync all of the interconnected stores on startup and it also starts the interval timer.
        node.statusTimerId = statusStores(node.statusTime); // This is the main call to display storeStatus info it starts the interval timer.
    } 
    catch (e) {
        sendMessage({ debug: e }, node);
    }

    function handleStoreStartError(e) {
        const errorMsg = `redlinkStore ${node.name} Error starting listen server on ${node.listenPort} Exception is ${e}`;
        sendMessage({debug: {storeName: node.name, action: 'startServer', error: errorMsg}}, node);
        clearInterval(node.statusTimerId); // Stop the status data being updated.
        clearInterval(node.reSyncTimerId); // Stop the store joining the mesh
        node.status({fill: "red", shape: "dot", text: 'Error: Listener please check Debug'});
    }

    if (node.listenPort) {
        try {
            const args = [+node.listenPort];
            if (node.isUserCertificate) {
                args.push(node.userKey);
                args.push(node.userCertificate);
            }
            node.listenServer = httpsServer.startServer(...args);
        } 
        catch (e) {
            handleStoreStartError(e);
        }

        if (node.listenServer) {
            node.listenServer.listen(node.listenPort).on('error', e => {
                if (e.code === 'EADDRINUSE') { handleStoreStartError(e); }
            });
            node.status({fill: "grey", shape: "dot", text: 'Initialising'});
        } 
      else 
        {
            handleStoreStartError('Unable to start server');
        }
        node.status({fill: "green", shape: "dot", text: 'Listen Port in OK'});
    }


    const app = httpsServer.getExpressApp();

    app.post('/notify', (req, res) => { // todo validation on params
        const notifyType = req.body.notifyType;
        const consumer = req.body.consumer;
        switch (notifyType) {
            case 'consumerRegistration' :
                sendMessage({ debug: { storeName: node.name, action: 'notifyConsumerRegistration', direction: 'inBound', data: req.body } }, node);
                
                const serviceName = consumer.serviceName;
                const consumerId = consumer.consumerId;
                const storeName = consumer.storeName;
                const direction = consumer.direction;
                const storeAddress = consumer.storeAddress;
                const storePort = consumer.storePort;
                const transitAddress = consumer.transitAddress;
                const transitPort = consumer.transitPort;
                const hopCount = consumer.hopCount || 0;
                const ttl = consumer.ttl;
                const ecm = 0;
                const erm = 0;
                const enm = 0;
                switch (direction) {
                    case 'store' : // Store only rego, this causes the southPeers list to update;
                        if (!node.southPeers.includes(transitAddress + ':' + transitPort)) {
                            node.southPeers.push(transitAddress + ':' + transitPort);
                            notifyAllSouthStoreConsumers(notifyDirections.SOUTH);                       // Pass the resistration to any other south store
                        } // Add this call as it is actually a store south calling this north store
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;

                    case 'reSync' : // Just insert or refresh the globalConsumers entries, no other action.
                        insertGlobalConsumer(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm, node.name);
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;
                    case 'local' :  // Connection is connecting from the local consumer
                        insertGlobalConsumer(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm, node.name);
                        notifyNorthStoreOfConsumers(consumer, storeAddress, storePort);  //  Pass the registration forward to the next store
                        consumer.hopCount = hopCount;
                        notifySouthStoreOfConsumers(consumer, notifyDirections.SOUTH, storeAddress, storePort, node.listenAddress, node.listenPort);
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;
                    case 'north' : // Connection is connecting from the South and this is why the routing is indicating that the serviceName is south of this store
                        if (node.southInsert) {
                            insertGlobalConsumer(serviceName, consumerId, storeName, 'south', storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm, node.name);
                            notifyNorthStoreOfConsumers(consumer, node.listenAddress, node.listenPort); // Pass the registration forward to the next store
                            notifyAllSouthStoreConsumers(notifyDirections.SOUTH);                       // Pass the resistration to any other south store
                        }
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;
                    case 'south' : // Connection is connecting from the North and this is why the routing is indicating that the serviceName is north of this store
                        insertGlobalConsumer(serviceName, consumerId, storeName, 'north', storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm, node.name);
                        if (node.northPeers.filter(x => x.ip === transitAddress && x.port === transitPort.toString()).length > 0) { //todo fix this
                            if (node.northPeers.filter(x => x.ip === transitAddress && x.port === transitPort.toString() && x.redistribute === 'true').length > 0) {
                                notifySouthStoreOfConsumers(consumer, notifyDirections.SOUTH, storeAddress, storePort, node.listenAddress, node.listenPort);
                            }
                            res.status(200).send({action: 'consumerRegistration', status: 200});
                        } 
                      else 
                        {
                          res.status(404).send({action: 'consumerRegistration', status: 404});
                        }
                        break;
                }
                break;
            case 'confirmNotification' :
                try {
                    // Once consumer selection has been done by the consumers store, then it is notified by popping the first element of the notifyPath and following the trail to the consumer
                    let myNotifyPath = req.body.tempPath; // Pop the first address:port off the front of the path as it's how it got here
                    myNotifyPath = myNotifyPath.slice(1);
                    if (myNotifyPath.length === 0) {
                        const matchingMessage = alasql(`SELECT sendOnly from inMessages WHERE redlinkMsgId="${req.body.redlinkMsgId}"`);
                        let sendOnly = false;
                        if (matchingMessage.length > 0) {
                            sendOnly = matchingMessage[0].sendOnly;
                        }
//                        const notifyInsertSql = `INSERT INTO notify VALUES ("${node.name}","${req.body.service}","${req.body.srcStoreAddress}",${req.body.srcStorePort},"${req.body.redlinkMsgId}","",false,"${req.body.redlinkProducerId}","${base64Helper.encode(req.body.notifyPath)}",${Date.now()},"${req.body.consumerId}",true, ${sendOnly})`;
                        const notifyInsertSql = `INSERT INTO notify VALUES ("${node.name}","${req.body.service}","${req.body.srcStoreName}","${req.body.srcStoreAddress}",${req.body.srcStorePort},"${req.body.redlinkMsgId}","",false,"${req.body.redlinkProducerId}","${base64Helper.encode(req.body.notifyPath)}",${Date.now()},"${req.body.consumerId}",true, ${sendOnly})`;
                        if (req.body.consumerId !== "") {
                            alasql(notifyInsertSql);
                            updateGlobalConsumerEnm(req.body.service, req.body.consumerId, node.name, calculateEnm(req.body.service, req.body.consumerId, node.name), node.name);
                            res.status(200).send({
                                action: 'confirmNotification :Inserted Notify ',
                                consumerId: req.body.consumerId,
                                enm: calculateEnm(req.body.service, req.body.consumerId, node.name),
                                service: req.body.service,
                                storeName: node.name,
                                status: 200
                            });
                        } 
                      else 
                        {
                            res.status(200).send({ action: 'confirmNotification :Inserted Notify No Consumer Specified', consumerId: req.body.consumerId, status: 404 });
                        }
                    } else { //Time to follow the trail
                        const path = myNotifyPath[0];
                        try {
                            req.body.tempPath = myNotifyPath;
                            const destinationStoreAddress = path.address + ":" + path.port;
                            const options = { method: 'POST', url: 'https://' + destinationStoreAddress + '/notify', body: req.body, json: true };
                            (async function () {
                                let response;
                                try { response = await requestPromise(options); } 
                                catch (e) {
                                    res.status(200).send({
                                        action: `Error posting to ${options.url} from ${node.name}`,
                                        consumerId: req.body.consumerId,
                                        notifyPath: [],
                                        status: 404
                                    });
                                    if (body) { // the body may not be there when a Deploy has caused a socket crash so only send if it exists
                                      sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: e } }, node);
                                    }
                                    return;
                                }
                                updateGlobalConsumerEnm(response.service, response.consumerId, response.storeName, response.enm, node.name);
                                res.status(200).send({
                                    action: 'REPLY confirmNotification forward to stores ' + node.name + ' remoteMatchingStores Result =',
                                    consumerId: response.consumerId,
                                    redlinkMsgId: response.redlinkMsgId,
                                    redlinkProducerId: response.redlinkProducerId,
                                    notifyPath: response.notifyPath,
                                    enm: response.enm,
                                    service: response.service,
                                    storeName: response.storeName,
                                    status: 200
                                });
                                return response;
                            })(); // This is the actual call to forward the notify , it causes recursive /producerNotifies lookups through the stores.
                        } 
                        catch (e) {
                            res.status(200).send({ action: `REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores ${node.name} remoteMatchingStores Result =`, consumerId: req.body.consumerId, notifyPath: [], status: 404 });
                        }
                    }
                } 
                catch (e) {
                    res.status(200).send({ action: `REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores ${node.name} remoteMatchingStores Result =`, consumerId: req.body.consumerId, notifyPath: [], status: 404 });
                }
                break;
            case 'producerNotification' :
                let notifyPath = [];
                req.body.notifyPath.forEach(function (path) { notifyPath.push(path); });
                sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'Received', data: req.body } }, node);
                let notifyConsumerId = ''; // Default Notify return, this store doesnt have the capacity to process this notification
                const existingLocalConsumerSql = `SELECT * FROM localStoreConsumers WHERE storeName="${node.name}" AND serviceName="${req.body.service}"`;
                const localCons = alasql(existingLocalConsumerSql);
                // Local Consumer Code Starts Here
                if (localCons.length > 0) {
                    let thisPath = { store: node.name, address: node.listenAddress, port: node.listenPort }; // Add my own store to the path, the producers need to to confirm the notification later
                    if (notifyPath.length > 0) { thisPath.enforceReversePath = notifyPath[0].enforceReversePath; } 
                                          else { thisPath.enforceReversePath = req.body.enforceReversePath }
                    notifyPath.push(thisPath);
                    // If this store has a local consumer on it then on confirmation send out a local notify, note that this store will terminate local service names here and will not forward them
                    const existingNotifySql = `SELECT * FROM notify WHERE storeName="${node.name}" AND serviceName="${req.body.service}" AND srcStoreAddress="${req.body.srcStoreAddress}" AND srcStorePort=${req.body.srcStorePort} AND redlinkMsgId="${req.body.redlinkMsgId}"`;
                    const existingNotify = alasql(existingNotifySql);
                    // If existing notify, then tell the producer
                    if (existingNotify && existingNotify.length > 0) {
                        notifyConsumerId = ''; // let this one go, already done.
                        sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'LocalConsumerNotifyDeniedAlreadyAccepted', data: req.body } }, node);
                        const enm = calculateEnm(req.body.service, consumer.notifyConsumerId, node.name);
                        res.status(200).send({
                            action: `Return back to producerNotification LocalConsumerNotifyDeniedAlreadyAccepted ${node.name}`,
                            consumerId: notifyConsumerId,
                            redlinkMsgId: req.body.redlinkMsgId,
                            redlinkProducerId: req.body.redlinkProducerId,
                            enm,
                            notifyPath,
                            consumerStore: req.body.storeName,
                            status: 404
                        });
                    } 
                  else 
                    {
                        // Lets see what the consumers capacity is for service
                        try {
                            sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'LocalConsumerNotifyAccepted', data: req.body } }, node);
                            // OK, now check for my consumer's capacity to actually queue this notify by seeing if the total consumers "inTransitLimit" is in limits.
                            //tODO this one is really wonky- need to fix this now!!!
                            // const enmClause =
                            const consumerIdsSql = `select consumerId,serviceName,enm from globalStoreConsumers WHERE localStoreName="${node.name}" AND storeName="${node.name}" AND serviceName="${req.body.service}" AND enm<100 order by enm limit 1`;
                            const consumerIds = alasql(consumerIdsSql);
                            if (consumerIds.length > 0) {
                                res.status(200).send({
                                    action: `Return back to producerNotification to consumer ${node.name}`,
                                    consumerId: consumerIds[0].consumerId, redlinkMsgId: req.body.redlinkMsgId,
                                    redlinkProducerId: req.body.redlinkProducerId,
                                    enm: consumerIds[0].enm,
                                    notifyPath,
                                    consumerStore: thisPath.store,
                                    status: 200
                                });
                            } 
                          else 
                            {
                                res.status(200).send({
                                    action: `Return back to producerNotification to consumer ${node.name}`,
                                    consumerId: "",
                                    redlinkMsgId: req.body.redlinkMsgId,
                                    redlinkProducerId: req.body.redlinkProducerId,
                                    enm: 100,
                                    notifyPath,
                                    consumerStore: thisPath.store,
                                    status: 404
                                });
                            }
                        } 
                        catch (e) {
                            res.status(200).send({
                                action: `Return back to producerNotification to consumer ${node.name}`,
                                consumerId: notifyConsumerId,
                                redlinkMsgId: req.body.redlinkMsgId,
                                redlinkProducerId: req.body.redlinkProducerId,
                                enm: 100,
                                notifyPath,
                                consumerStore: req.body.storeName,
                                status: 404
                            });
                        }
                    }
                    break;
                } 
              else 
                {
                    // Transit Notify Code starts here
                    // Note that the reverse path state is set in the first notify, so, we can use it to set subsequent notifies
                    let thisPath = { store: node.name, address: node.listenAddress, port: node.listenPort };
                    if (notifyPath.length > 0) { thisPath.enforceReversePath = notifyPath[0].enforceReversePath; } 
                                          else { thisPath.enforceReversePath = req.body.enforceReversePath; }
                    notifyPath.push(thisPath);
                    const remoteMatchingStores = [...new Set([...getRemoteMatchingStores(req.body.service, node.meshName, node.interStoreLoadBalancer, req.body.enforceReversePath)])];
                    if (remoteMatchingStores.length === 0) { // this can be empty- there may not be a matching store as registrations may still be in progress- in case of redeploy for example
                        res.status(200).send({
                            action: `REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores ${node.name} remoteMatchingStores Result =${remoteMatchingStores}`,
                            consumerId: notifyConsumerId,
                            enm: 100,
                            notifyPath,
                            status: 404
                        });
                        return;
                    }
                    try {
                        // This is the interstore notifier
                        req.body.notifyPath = notifyPath;
                        let remoteStore = remoteMatchingStores[0];
                        let destinationStoreAddress = remoteStore.transitStoreAddress;
                        const options = { method: 'POST', url: 'https://' + destinationStoreAddress + '/notify', body: req.body, json: true };
                        (async function () {
                            try {
                                let response;
                                try { response = await requestPromise(options); } 
                                catch (e) {
                                        res.status(200).send({ action: `Error posting to ${options.url} from ${node.name}`, status: 404 });
                                    sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: e } }, node);
                                    return;
                                }
                                if (response.consumerId !== "") {
                                    // Set the ecm value here on response for this store
                                    // updateGlobalConsumerEnm(req.body.service, response.consumerId, response.consumerStore, response.enm);
                                }
                                res.status(200).send({
                                    action: 'REPLY producerNotification forward to stores ' + node.name + ' remoteMatchingStores Result =' + remoteMatchingStores,
                                    consumerId: response.consumerId,
                                    redlinkMsgId: response.redlinkMsgId,
                                    redlinkProducerId: response.redlinkProducerId,
                                    enm: response.enm,
                                    notifyPath: response.notifyPath,
                                    consumerStore: response.consumerStore,
                                    status: response.status
                                });
                            } 
                            catch (e) {
                            // This error is thrown when large numbers of notifies are in progress and the stores are cleared by doing Deploys etc.
                            // The response below is now not sent, found that it just threw extra errors in the express/lib/response.js code.
                            /*
                                res.status(200).send({ action: 'Error producerNotification forward to stores ' + node.name + ' Connection Reset', status: 404 });
                             */   
                             //console.log('Connection Reset'); 
                            }
                        })(); // This is the actual call to forward the notify , it causes recursive /producerNotifies lookups through the stores.
                    } 
                    catch (e) {
                        res.status(200).send({
                            action: 'REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores ' + node.name + ' remoteMatchingStores Result =' + remoteMatchingStores,
                            consumerId: notifyConsumerId,
                            enm: 100,
                            notifyPath,
                            status: 404
                        });
                    }
                    break;
                }
        } // case
    }); // notify


    app.post('/read-message', (req, res) => {
        // This function allows path recursion backwards to producer
        let notifyPathIn = req.body && req.body.notifyPath ? base64Helper.decode(req.body.notifyPath) : [];
        const notifyPath = notifyPathIn.pop();
        if (notifyPath) {
            if (notifyPath.address !== node.listenAddress || notifyPath.port !== node.listenPort) {
                updateGlobalConsumerEnm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.enm, node.name);
                updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay, node.name);
                updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay, node.name); // Dont know if the consumer actually read the job, so, set to the same read score.
                const body = req.body;
                body.notifyPath = base64Helper.encode(notifyPathIn);
                const options = { method: 'POST', url: 'https://' + notifyPath.address + ':' + notifyPath.port + '/read-message', body, json: true };
                sendMessage({ debug: { storeName: node.name, action: 'message-read-forwarding', direction: 'relayingBackToProducer', data: options } }, node);
                request(options, function (error, response) {
                    if (response) {
                        const redlinkMsgMetadata = pick(response.headers, Object.keys(messageFields));
                        res.set(redlinkMsgMetadata);
                        res.status(200).send(response.body);
                    } 
                  else 
                    {
                        res.status(200).send({error: `no response from ${options.url} when message-read-forwarding`});
                    }
                });
            }
        } 
      else 
        {
            const redlinkMsgId = req.body.redlinkMsgId;
            const redlinkProducerId = req.body.redlinkProducerId;
            const redlinkReadDelay = req.body.readDelay;
            const redlinkConsumerId = req.body.consumerId;
            sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'inBound', Data: req.body } }, node);
            if (!redlinkMsgId) {
                sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', error: 'redlinkMsgId not specified-400' } }, node);
                res.status(200).send({err: 'redlinkMsgId not specified', redlinkProducerId, status: 400});
                return;
            }
            const msgSql = `SELECT * FROM inMessages WHERE redlinkMsgId="${redlinkMsgId}" AND read=false AND consumerId ="${redlinkConsumerId}"`;
            const msgs = alasql(msgSql);// should get one or none
            if (msgs.length > 0) { // will be zero if the message has already been read
                sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', data: msgs[msgs.length - 1], error: 'none' } }, node);
                const message = msgs[0];
                const headersObj = {};
                Object.assign(headersObj, message);
                delete headersObj.message;
                res.set(headersObj);
                const isLargeMessage = message.isLargeMessage;
                if (isLargeMessage) {
                    const path = largeMessagesDirectory + redlinkMsgId + '/message.txt';
                    const src = fs.createReadStream(path);
                    src.pipe(res);
                    src.on('end', () => {
                        fs.removeSync(path);
                    });
                } 
              else 
                {
                    const src = new Readable();
                    src.push(message.message);
                    src.push(null);
                    src.pipe(res);
                }
                //This ecm is calculated in and by the consumer, it is the delay between receiving the notify and then performing this read, manual reads drastically increase this time in mS.
                updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay, node.name);
                updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, 0, node.name); //Hasnt replied yet, so, no scrore
                const updateMsgStatus = `UPDATE inMessages SET read=true WHERE redlinkMsgId="${msgs[0].redlinkMsgId}"`;
                alasql(updateMsgStatus);
                msgs[0].status = 200;
                if (msgs[0].sendOnly) {            // delete if send only
                    const deleteMsgSql = `DELETE FROM inMessages WHERE redlinkMsgId="${redlinkMsgId}"`;
                    alasql(deleteMsgSql);
                    updateGlobalConsumerEnm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.enm, node.name);
                    notifyUnreadMessages(); // <------------------------------------------------------------
                } 
              else 
                {
                    // update message to read=true , as this consumer won the message
                    const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + msgs[0].redlinkMsgId + '"';
                    alasql(updateMsgStatus);
                }
            } 
          else 
            { // Message has already been read.
                updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay, node.name);
                updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay, node.name); // Didnt get to do the job, so, give the reply the same read score.
                const msg = redlinkMsgId ? `Message with id ${redlinkMsgId} not found- it may have already been read` : 'No unread messages';
                sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', error: msg } }, node);
                res.status(200).send({err: msg, redlinkProducerId, redlinkConsumerId, status: 404});
            }
        }
    });

    app.post('/reply-message', (req, res) => {
        let notifyPathIn = req.body && req.body.notifyPath ? base64Helper.decode(req.body.notifyPath) : [];
        const notifyPath = notifyPathIn.pop();
        // This function allows path recusion backwards to producer
        if (notifyPath) {
            if (notifyPath.address !== node.listenAddress || notifyPath.port !== node.listenPort) {
                updateGlobalConsumerEnm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.enm, node.name);
                updateGlobalConsumerErm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.replyDelay, node.name); // Update the is score.
                const body = req.body;
                body.notifyPath = base64Helper.encode(notifyPathIn);
                const options = { method: 'POST', url: 'https://' + notifyPath.address + ':' + notifyPath.port + '/reply-message', body, json: true };
                sendMessage({ debug: { storeName: node.name, action: 'message-reply-forwarding', direction: 'relayingBackToProducer', data: options } }, node);
                request(options, function (error, response) {
                    if (response) {
                        res.status(response.statusCode).send(response.body);
                    } 
                  else 
                    {
                        res.status(404).send({error});
                        sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error } }, node);
                    }
                });
            }
        } 
      else 
        {
          notifyUnreadMessages();
          updateGlobalConsumerEnm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.enm, node.name);
          updateGlobalConsumerErm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.replyDelay, node.name); // Update the reply score.
          const redlinkMsgId = req.body.redlinkMsgId;
          const redlinkProducerId = req.body.redlinkProducerId;
          const message = req.body.payload;
          let cerror = '';
          if (req.body.cerror) { cerror = req.body.cerror }
          sendMessage({ debug: { storeName: node.name, action: 'reply-message', direction: 'inBound', Data: req.body } }, node);
          const replyMsgSql = `SELECT DISTINCT * FROM replyMessages WHERE redlinkMsgId="${redlinkMsgId}"`;
          const replyMsg = alasql(replyMsgSql);
          if (replyMsg.length === 0) {
             let insertReplySql;
             if (isLargeMessage(message)) {
                 //store reply to disk
                 const path = largeMessagesDirectory + redlinkMsgId + '/';
                 fs.outputFileSync(path + 'reply.txt', message);
                 insertReplySql = `INSERT INTO replyMessages ("${node.name}","${redlinkMsgId}","${redlinkProducerId}","", false, true,"${cerror}")`;
             } 
           else 
             {
               insertReplySql = `INSERT INTO replyMessages ("${node.name}","${redlinkMsgId}","${redlinkProducerId}","${message}", false, false,"${cerror}")`;
             }
             alasql(insertReplySql);
             res.status(200).send({msg: `Reply received for ${redlinkMsgId}`, redlinkMsgId}); //todo fix error message
         } 
       else 
         {
           res.status(404).send({msg: `Reply Rejected for ${redlinkMsgId}`, redlinkMsgId});
         }
        }
    });

    function deleteOldConsumers() {
        const selectSql = `select * from globalStoreConsumers where localStoreName = "${node.name}" and ttl <= ${Math.floor(new Date().getTime() / 1000)}`;
        const deleteSql = `delete   from globalStoreConsumers where localStoreName = "${node.name}" and ttl <= ${Math.floor(new Date().getTime() / 1000)}`;
        const selectData = alasql(selectSql);
        if (selectData && selectData.length > 0) {
            sendMessage({ registration: { storeName: node.name, action: 'deleteConsumer', direction: 'outBound', data: {record: selectData, time: Math.floor(new Date().getTime() / 1000)}, error: 'none' } }, node);
            alasql(deleteSql);
        }
    }

    function reSyncStores(timeOut) {
        return setInterval(function () {
            // First get any local consumers that I own and update my own global entries in my own store, this updates ttl.
            notifyPeerStoreOfLocalConsumers(node.listenAddress, node.listenPort, node.listenAddress, node.listenPort);
            deleteOldConsumers();              // Next  clean up my store first to remove old entries that have not renewed themselves
            notifyAllNorthPeerStoresOnly();    // Make sure the north store knows about me.
        }, timeOut);
    }

    function statusStores(timeOut) {
        return setInterval(function () {
            const consumers = getAllVisibleConsumers();
            node.status({ shape: "dot", text: `Local(${consumers.localConsumers.length}) Global(${consumers.globalConsumers.length})`, fill: consumers.localConsumers.length > 0 ? "green" : "yellow" }); 
        }, timeOut);
    }

    function isLargeMessage(encodedReplyMessage) {
        return encodedReplyMessage.length > largeMessageThreshold;
    }

    function getAllVisibleServices() {
        const globalConsumersSql = `SELECT serviceName FROM globalStoreConsumers WHERE localStoreName="${node.name}"`;
        const globalConsumers = alasql(globalConsumersSql);
        let myServices = [];
        globalConsumers.forEach(consumer => { myServices.push(consumer.serviceName); });
        return myServices;
    }

    function getAllVisibleConsumers() {
        const localConsumers = alasql(`SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="${node.name}"`);
        const globalConsumers = alasql(`SELECT * FROM globalStoreConsumers WHERE localStoreName="${node.name}"`);
        const store = alasql(`SELECT * FROM stores where storeName ="${node.name}"`);
        const peers = {"northPeers": node.northPeers, "southPeers": node.southPeers};
        return { localConsumers, globalConsumers, store, peers };
    }

    function getCurrentStoreData() {
        const store = alasql(`SELECT * FROM stores where storeName ="${node.name}"`);
        const messages = alasql(`SELECT * FROM inMessages where storeName ="${node.name}"`);
        const notifies = alasql(`SELECT * FROM notify where storeName ="${node.name}"`);
        const replies = alasql(`SELECT * FROM replyMessages where storeName ="${node.name}"`);
        return { store, messages, notifies, replies };
    }

    node.on("input", msg => {
        switch (msg.topic) {
            case 'listServices'      : { sendMessage({command: {services: getAllVisibleServices()}}, node); break; }
            case 'listRegistrations' : { sendMessage({command: getAllVisibleConsumers()}, node); break; }
            case 'listStore'         : { sendMessage({command: getCurrentStoreData()}, node); break; }
            case 'flushStore'        : {
                alasql(`DELETE FROM replyMessages WHERE storeName="${node.name}"`);
                alasql(`DELETE FROM notify WHERE storeName="${node.name}"`);
                alasql(`DELETE FROM inMessages WHERE storeName="${node.name}"`);
                sendMessage({command: getCurrentStoreData()}, node);
                break;
            }
            default                  : { sendMessage({command: {help: "msg.topic can be listRegistrations listStore flushStore"}}, node); break; }
        }
    });

    node.on('close', (removed, done) => {
        closeStore(node, done);
    });
}; // function

