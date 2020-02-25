const httpsServer = require('./https-server.js');
const alasql = require('alasql');
const request = require('request-promise-native').defaults({strictSSL: false});
const fs = require('fs-extra');
const base64Helper = require('./base64-helper.js');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkStore = function (config) {
    const notifyDirections = {
        NORTH: 'north',
        SOUTH: 'south',
        LOCAL: 'local',
        STORE: 'store',
        RESYN: 'reSync'
    };

    RED.nodes.createNode(this, config);
    const node = this;
    const log = require('./log.js')(node).log;

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
    const largeMessagesDirectory = require('./redlinkSettings.js')(RED, node.name).largeMessagesDirectory;
    const largeMessageThreshold = require('./redlinkSettings.js')(RED, node.name).largeMessageThreshold;

    //console.log(node.name ,'(',node.northPeers,') = ',node.northPeers[0].redistribute);
    // Insert myself into the mesh.
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    alasql(insertStoreSql);

    function sendMessage(msg) { // command, registration, debug
        const msgs = [];
        if (node.command) { msgs.push(msg.command);  }
        if (node.registration) { msgs.push(msg.registration); }
        if (node.debug) { msgs.push(msg.debug); }
        node.send(msgs);
    }



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
                ttl: Math.floor(new Date().getTime()/1000)+node.consumerlifeSpan // Setup the overall lifetime for this service, its epoch secs + overall lifespan in secs
            });

            let consumer = qualifiedLocalConsumers[0]; // todo filter this for unique consumers
            let body = { consumer, notifyType: 'consumerRegistration' };

            if (address && address !== '0.0.0.0') {
                sendMessage({ registration: { storeName: node.name, action: 'notifyRegistration', function: 'notifyPeerStoreOfLocalConsumers', notifyData: body } });
                sendMessage({ debug: { storeName: node.name, action: 'notifyRegistration', direction: 'outBound', notifyData: body } });
                const options = {method: 'POST', url: 'https://' + address + ':' + port + '/notify', body, json: true};
                request(options, function (error, response) {
                    if (error) { sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: error } });
                    } else {     sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: response.body      } });
                    }
                });
            }
        });
    }

    function deleteSouthPeer (address,port,peer) {
      return peer.filter(function(addressPort) {
        return addressPort != address+':'+port;
        });
    }


    function deleteGlobalStoreConsumers(store, direction, address, port) {
      const deleteSql = 'delete from globalStoreConsumers where localStoreName="'+store+'" and direction="'+direction+'" and transitAddress="'+address+'" and transitPort='+port;
      const deleteResult = alasql(deleteSql);
      return (deleteResult > 0);
    }

    function notifyPeerStoreOfConsumers(consumer, direction, hopCount, address, port, transitAddress, transitPort) {
        consumer.direction = direction;
        consumer.transitAddress = transitAddress;
        consumer.transitPort = transitPort;
        consumer.hopCount = hopCount;
        let body = { consumer, notifyType: 'consumerRegistration' };

        if (address && address !== '0.0.0.0') {
            sendMessage({ debug: { storeName: node.name, action: 'notifyRegistration', direction: 'outBound', notifyData: body } });
            const options = {method: 'POST', url: 'https://' + address + ':' + port + '/notify', body, json: true};

            request(options, function (error, response) {
               if (error) {
                    if (direction === 'south') {
                      node.southPeers = deleteSouthPeer(address,port, node.southPeers); // Ok, looks like the peer has gone, so, lets delete the peer entry.
                      deleteGlobalStoreConsumers(node.name, direction, address, port);
                      sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConnection', direction: direction, notifyData: address+':'+port, serviceName : consumer.serviceName}});
                    }
             else
               if (direction === 'north') { // Ok, North Peers are hard wired, if the connection is a problem, then just delete the globalStoreConsumers.
                 if (deleteGlobalStoreConsumers(node.name, direction, address, port)){
                    sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConsumer',   direction: direction, notifyData: address+':'+port, serviceName : consumer.serviceName}});
                   }
                 }
                 sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: body, error: error }});
               }
             else
               if (direction === 'south' && response.statusCode === 404) {
                 node.southPeers = deleteSouthPeer(address,port, node.southPeers); // Ok, looks like the peer has rejected the update, so, lets delete the peer entry.
                 deleteGlobalStoreConsumers(node.name, direction, address, port);
                 sendMessage({ registration: { storeName: node.name, action: 'notifyDeletePeerConnection', direction: direction, notifyData: address+':'+port, serviceName : consumer.serviceName}});
                    }
             else
               {
                 sendMessage({ debug: { storeName: node.name, action: 'notifyRegistrationResult', direction: 'outBound', notifyData: response.body } });
               }
            });
 
        }
    }


    function notifyAllNorthPeerStoresOnly() {
        let consumer = {};
        consumer.direction = 'store';
        consumer.transitAddress = node.listenAddress;
        consumer.transitPort = node.listenPort;
        let body = { consumer, notifyType: 'consumerRegistration' };
        node.northPeers.forEach(peer => { notifyPeerStoreOfConsumers(consumer, consumer.direction, 0, peer.ip, peer.port, consumer.transitAddress, consumer.transitPort); });
    }


    function notifyNorthStoreOfConsumers(consumer, transitAddress, transitPort) {    
        const newHopCount = consumer.hopCount+1;
        node.northPeers.forEach(peer => { 
           if (consumer.transitAddress+':'+consumer.transitPort != peer.ip+':'+peer.port) { notifyPeerStoreOfConsumers(consumer, notifyDirections.NORTH, newHopCount, peer.ip, peer.port, transitAddress, transitPort); }
        });
    }


    function notifySouthStoreOfConsumers(consumer, direction, storeAddress, storePort, transitAddress, transitPort) {
        const newHopCount = consumer.hopCount+1;
        node.southPeers.forEach(peer => {
            var [ip, port] = peer.split(':');
            if (consumer.storeAddress+':'+consumer.storePort !== ip+':'+port) { notifyPeerStoreOfConsumers(consumer, direction, newHopCount, ip, port, transitAddress, transitPort); }
        });
    }

    function notifyAllSouthStoreConsumers(direction) {
        const storeName = node.name;
        const meshName = storeName.substring(0, storeName.indexOf(':')); // Producers can only send to Consumers on the same mesh
        const storeAddressData = alasql('SELECT * FROM stores  WHERE storeName = "' + storeName + '"');
        const globalConsumersSql = 'SELECT  DISTINCT serviceName,consumerId from ( select * from globalStoreConsumers WHERE localStoreName = "' + storeName + '")';// +
        const globalConsumers = alasql(globalConsumersSql);

        const allConsumers = [...new Set([...globalConsumers])];

        const storeAddress = storeAddressData[0].storeAddress;
        const storePort = storeAddressData[0].storePort;
        allConsumers.forEach(consumer => {
            const dataSql = 'select * from globalStoreConsumers where localStoreName = "' + storeName + '" and serviceName ="' + consumer.serviceName + '" and consumerId = "' + consumer.consumerId + '"';
            const data = alasql(dataSql);
            notifySouthStoreOfConsumers(data[0], direction, storeAddress, storePort, node.listenAddress, node.listenPort);
        });
    }


    const nodeId = config.id.replace('.', '');
    const newMsgTriggerName = 'onNewMessage' + nodeId;
    const registerConsumerTriggerName = 'registerConsumer' + nodeId;

    
    function getRemoteMatchingStores(serviceName, meshName, loadBalancer, useEnm) {
        let globalStoresSql = "";
        let matchingGlobalStoresAddresses = [];
        if (useEnm) {globalStoresSql = 'SELECT * from (select * FROM globalStoreConsumers WHERE serviceName="' + serviceName + '" AND localStoreName = "' + node.name + '" order by enm ) group by transitAddress,transitPort';}
              else  {globalStoresSql = 'SELECT * from (select * FROM globalStoreConsumers WHERE serviceName="' + serviceName + '" AND localStoreName = "' + node.name + '" order by enm ) group by transitAddress,transitPort';}  

        if (loadBalancer) {    
           const matchingGlobalStores = alasql(globalStoresSql);

           if (matchingGlobalStores.length > 0) {
              matchingGlobalStoresAddresses.push({
                transitStoreAddress: matchingGlobalStores[0].transitAddress + ':' + matchingGlobalStores[0].transitPort,
                transitHopCount: matchingGlobalStores[0].hopCount,
                transitEcm : matchingGlobalStores[0].ecm,
                transitErm : matchingGlobalStores[0].erm,
                transitEnm : matchingGlobalStores[0].enm
              });
           }
         else
          matchingGlobalStoresAddresses = [];
        }
      else
        { // No Load Balancer on this store
           const matchingGlobalStores = alasql(globalStoresSql);
           matchingGlobalStores.forEach(store => {
              matchingGlobalStoresAddresses.push({
                transitStoreAddress: store.transitAddress + ':' + store.transitPort,
                transitHopCount: store.hopCount,
                transitEcm : store.ecm,
                transitErm : store.erm,
                transitEnm : store.enm
              });
          });
        }        
        return matchingGlobalStoresAddresses;
    }


    function NotifyEveryMsgNotRead() {

            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '" AND read= false AND consumerId = ""  ORDER BY priority DESC limit 1';
            var newMessages = alasql(newMessagesSql);
            const newMessage = newMessages[0];

            if (newMessage) { 

               sendMessage({
                  registration: { // todo rename to notify
                    service: newMessage.serviceName,
                    srcStoreAddress: node.listenAddress,
                    srcStorePort: node.listenPort,
                    redlinkMsgId: newMessage.redlinkMsgId,
                    action: 'producerNotification',
                    redlinkProducerId: newMessage.redlinkProducerId
                   }
               });
               const updateMsgStatus = 'UPDATE inMessages SET consumerId ="pending"  WHERE consumerId = "" AND redlinkMsgId="' +  newMessage.redlinkMsgId + '" AND storeName = "'+node.name+'"';
               alasql(updateMsgStatus);
                
               let remoteMatchingStores = [];
            
               if (newMessage.sendOnly) {remoteMatchingStores = [...new Set([...getRemoteMatchingStores(newMessage.serviceName, node.meshName, node.interStoreLoadBalancer, true)])];}
                                  else  {remoteMatchingStores = [...new Set([...getRemoteMatchingStores(newMessage.serviceName, node.meshName, node.interStoreLoadBalancer, false)])];}
            
               let producerStores = remoteMatchingStores;

               let thisPath = []; // Effectively sets the start path to this store, as the notify adds it in the /notify code
              
               producerStores.forEach(remoteStore => {
                  let body = {
                      service: newMessage.serviceName,
                      srcStoreAddress: node.listenAddress,
                      srcStorePort: node.listenPort,
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


                  const options = { method: 'POST', url: 'https://' + node.listenAddress+':'+node.listenPort + '/notify', body, json: true };

                  // Ok Ask the Consumer if they have the capacity to consume this message
                  async function doStuff() {
                     const awaitOut = await request(options).then (response => {
                                        // Ok, if a consumerId is not empty, then set the message with the consumerId and use the returned path to go directly to the consumer store.
                                        if (response.consumerId != '') {
                                           const updateMsgStatus = 'UPDATE inMessages SET consumerId ="' + response.consumerId + '"  WHERE consumerId = "pending" AND redlinkMsgId="' + 
                                                                   body.redlinkMsgId + '" AND storeName = "'+node.name+'"';
                                           const updated = alasql(updateMsgStatus);    
                                        }  

                                        // Ok, if a consumerId is not empty, then use the returned path to go directly to the consumer store and confirm the message, this will cause the consumer store to notify the consumerId 
                                        if (response.consumerId != '') {
                                           const destinationAddress = response.notifyPath[0];
                                           let confirmBody = body;
                                           confirmBody.notifyPath = response.notifyPath;
                                           confirmBody.consumerId = response.consumerId;
                                           confirmBody.notifyType = 'confirmNotification';
                                           confirmBody.tempPath   = response.notifyPath; // This is used to traverse to the consumer store again, and it is removed once it gets there.
                                           body = confirmBody;
                                           const options = { method: 'POST', url: 'https://' + node.listenAddress+':'+node.listenPort + '/notify', body, json: true }
                                           //  Recurse to this remoteStore
                                           async function doStuff2() {
                                             const awaitOut2 = await request(options).then (response => {
                                                 //console.log('Final Response = ',response);
                                                 if (response.consumerId != '') { 
                                                     // OK, I have a winner

                                                 }  
                                                 return response;
                                                 }).catch(myError02 => {
                                                   console.log('myError02=',myError02);
                                             });
                                             return awaitOut2;
                                           }
                                           doStuff2();
                                        }
                                        return response;
                                      }).catch(myError0 => {
                                         console.log('myError0=',myError0);
                                      });
                     return awaitOut;
                  }      

                  let myResult=  doStuff(); // This is the actual call to get the notify going, it causes recursive /producerNotifies lookups through the stores.
                  //console.log('----------------------- Sending ',body.redlinkMsgId,' --------------------------------');   
               });
           }
    }

    //---------------------------------------------------  Notify Triggers  ---------------------------------------------------------
    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
           // console.log(node.name,'   NotifyEveryMsgNotRead');        
           NotifyEveryMsgNotRead();
           };


        // On local consumer registration, let them all know
        alasql.fn[registerConsumerTriggerName] = () => {
            // Notify my local globalConsumerStore with a default hop count of zero and a transit address of myself...all the localConsumers.
            notifyPeerStoreOfLocalConsumers(node.listenAddress, node.listenPort, node.listenAddress, node.listenPort);
           };

        const createNewMsgTriggerSql =    'CREATE TRIGGER ' + newMsgTriggerName +           ' AFTER INSERT ON inMessages CALL ' +          newMsgTriggerName + '()';
        const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName + ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
        try {
            alasql(createNewMsgTriggerSql);
            alasql(createRegisterConsumerSql);
            } 
        catch (e1) {
           }

        notifyAllNorthPeerStoresOnly(); // Register myself with my North Store
        node.reSyncTimerId = reSyncStores(node.reSyncTime); // This is the main call to sync all of the interconnected stores on startup and it also starts the interval timer.
        node.statusTimerId = statusStores(node.statusTime); // This is the main call to display storeStatus info it starts the interval timer.

    } 
    catch (e) {
        log(e);
    }

    function handleStoreStartError(e) {
        const errorMsg = 'redlinkStore ' + node.name + ' Error starting listen server on ' + node.listenPort + ' Exception is ' + e;
        sendMessage({ debug: { storeName: node.name, action: 'startServer', error: errorMsg } });
        clearInterval(node.statusTimerId); // Stop the status data being updated.
        clearInterval(node.reSyncTimerId); // Stop the store joining the mesh
        node.status({fill: "red", shape: "dot", text: 'Error: Listener please check Debug'});
    }

    if (node.listenPort) {
        try {
              const args = [+node.listenPort];
              if( node.isUserCertificate ) {
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
          node.status({fill: "grey", shape: "dot", text:'Initialising'});
        } 
      else
        {
          handleStoreStartError('Unable to start server');
        }
        node.status({fill: "green", shape: "dot", text: 'Listen Port in OK'});
        // log('started server at port:', node.listenPort);
    }


    function insertGlobalConsumer(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId +
            '" AND storeName="' + storeName + '" AND storeAddress = "' + storeAddress + '" AND storePort = ' + storePort;

        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);

        const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + consumerId + '","' + storeName + '","' + direction + '","' +
            storeAddress + '",' + storePort + ',"' + transitAddress + '",' + transitPort + ',' + hopCount + ',' + ttl + ','+ ecm + ',' + erm + ',' +enm +')';
        if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
            const inserted = alasql(insertGlobalConsumersSql);
        } else {
            const updateConsumerTtl = 'UPDATE globalStoreConsumers SET ttl=' + ttl + '  WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND consumerId="' + consumerId +
                                      '" AND storeName="' + storeName + '" AND storeAddress = "' + storeAddress + '" AND storePort = ' + storePort;
            alasql(updateConsumerTtl);
               // console.log('ServiceName ',serviceName,'  Exists and TTl updated=',alasql(updateConsumerTtl));
               // Possibly Need to add a delete and an insert here for lower hopCount routes, it will reduce the number of notifies on high complexity redlink store layouts.
               // console.log('Insert Failed, globalStore ',node.name, 'service ',serviceName,' has an entry with a hopCount of ',existingGlobalConsumer[0].hopCount,' compared with ',hopCount);
        }
    }

    
    function updateGlobalConsumerEnm(serviceName, consumerId, storeName, enm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '"' ;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerEnm = 'UPDATE globalStoreConsumers SET enm=' + enm + ' WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '" AND consumerId="' + consumerId +'"';
            alasql(updateConsumerEnm);
        }
    }

    function updateGlobalConsumerEcm(serviceName, consumerId, storeName, ecm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '"' ;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerEcm = 'UPDATE globalStoreConsumers SET ecm=' + ecm + ' WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '" AND consumerId="' + consumerId +'"' ;
            alasql(updateConsumerEcm);
        }
    }


    function updateGlobalConsumerErm(serviceName, consumerId, storeName, erm) {
        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '"' ;
        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
        if (existingGlobalConsumer) {
            const updateConsumerErm = 'UPDATE globalStoreConsumers SET erm=' + erm + ' WHERE localStoreName="' + node.name + '" AND serviceName="' + serviceName + '" AND storeName="' + storeName + '"AND consumerId="' + consumerId +'"' ;
            alasql(updateConsumerErm);
        }
    }


    function deleteNotify(redlinkMsgId) {
        const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' + redlinkMsgId + '" and storeName = "' + node.nam+'"';
//        console.log('Delete Notify Store = ',node.name,'  msgId = ',redlinkMsgId);
        return alasql(deleteNotifyMsg);
    }

   function calculateEnm(serviceName,consumerId) {
       const existingLocalConsumerSql = 'SELECT * FROM localStoreConsumers   WHERE storeName="' + node.name + '" AND serviceName="' + serviceName + '" AND consumerId="'+consumerId+'"';
       const notifyCountSql = 'select count(*) nc from (SELECT * FROM notify WHERE storeName="' + node.name + '" AND serviceName="' + serviceName + '" AND consumerId="'+consumerId+'")';
       const nc = alasql(notifyCountSql)[0].nc;
       const itlSql = 'select inTransitLimit itl from ('+existingLocalConsumerSql+')';
       const itl = alasql(itlSql)[0].itl;
       //console.log(consumerId,'=',((nc)/itl*100).toFixed(0));
       return ((nc)/itl*100).toFixed(0);
   }

    const app = httpsServer.getExpressApp();

    app.post('/notify', (req, res) => { // todo validation on params
        const notifyType = req.body.notifyType;
//        console.log(req.body);
//        console.log('NOTIFICATION TYPE = ',notifyType);
        switch (notifyType) {

            case 'consumerRegistration' :

                sendMessage({ debug: { storeName: node.name, action: 'notifyConsumerRegistration', direction: 'inBound', data: req.body } });

                const consumer = req.body.consumer;
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
                        insertGlobalConsumer(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm);
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;

                    case 'local' :  // Connection is connecting from the local consumer
                        insertGlobalConsumer(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm);
                        notifyNorthStoreOfConsumers(consumer, storeAddress, storePort);  //  Pass the registration forward to the next store
                        consumer.hopCount = hopCount;
                        notifySouthStoreOfConsumers(consumer, notifyDirections.SOUTH, storeAddress, storePort, node.listenAddress, node.listenPort);
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;

                    case 'north' : // Connection is connecting from the South and this is why the routing is indicating that the serviceName is south of this store
                        if (node.southInsert) {
                           insertGlobalConsumer(serviceName, consumerId, storeName, 'south', storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm);
                           notifyNorthStoreOfConsumers(consumer, node.listenAddress, node.listenPort); // Pass the registration forward to the next store
                           notifyAllSouthStoreConsumers(notifyDirections.SOUTH);                       // Pass the resistration to any other south store
                        }
                        res.status(200).send({action: 'consumerRegistration', status: 200});
                        break;

                    case 'south' : // Connection is connecting from the North and this is why the routing is indicating that the serviceName is north of this store
                        insertGlobalConsumer(serviceName, consumerId, storeName, 'north', storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm);
                        if (node.northPeers.filter( x=> x.ip == transitAddress && x.port == transitPort.toString() ).length > 0) {
                          if (node.northPeers.filter( x=> x.ip == transitAddress && x.port == transitPort.toString() &&  x.redistribute=='true').length > 0) {
                             notifySouthStoreOfConsumers(consumer, notifyDirections.SOUTH, storeAddress, storePort, node.listenAddress, node.listenPort);
                             }
                          res.status(200).send({action: 'consumerRegistration', status: 200});
                        }
                        else {
                          res.status(404).send({action: 'consumerRegistration', status: 404});
                        }  
                        break;
                }

                break;

            case 'confirmNotification' :
                // Once consumer selection has been done by the consumers store, then it is notified by poping the first element of the notifyPath and following the trail to the consumer
                let   myNotifyPath = req.body.tempPath; // Pop the first address:port off the front of the path as it's how it got here
                myNotifyPath = myNotifyPath.slice(1);
                if (myNotifyPath.length == 0) {
                  const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreAddress + '",' + req.body.srcStorePort + ',"' +
                                          req.body.redlinkMsgId + '","",false,"' + req.body.redlinkProducerId + '","'+ base64Helper.encode(req.body.notifyPath)+'",'+Date.now()+',"'+req.body.consumerId+'",true)';
                  if (req.body.consumerId != "") {
                    alasql(notifyInsertSql);
                    res.status(200).send({action: 'confirmNotification :Inserted Notify ', consumerId: req.body.consumerId, status: 200});
                    updateGlobalConsumerEnm(req.body.service, req.body.consumerId, node.name, calculateEnm(req.body.service,req.body.consumerId));
                  }
                else
                  {
                    res.status(200).send({action: 'confirmNotification :Inserted Notify No Consumer Specified', consumerId: req.body.consumerId, status: 404});
                  }   
                }
              else
                { //Time to follow the trail
                  const path = myNotifyPath[0];
                  try {
                        req.body.tempPath = myNotifyPath;
                        const destinationStoreAddress = path.address+":"+path.port;
                        const options = { method: 'POST', url: 'https://' + destinationStoreAddress + '/notify', body:req.body, json: true };

                        async function doStuff() {
                           const awaitOut = await request(options).then (response => {
                              res.status(200).send({action: 'REPLY confirmNotification forward to stores '+node.name+ ' remoteMatchingStores Result =', 
                                                   consumerId: response.consumerId, redlinkMsgId : response.redlinkMsgId, redlinkProducerId : response.redlinkProducerId, notifyPath : response.notifyPath, status: 200});
                              return response;
                           });
                           return awaitOut;
                        }
                        doStuff(); // This is the actual call to forward the notify , it causes recursive /producerNotifies lookups through the stores.
                      }      
                   catch(e)
                      {
                       //console.log('This should not happen NOTIFY TRANSIT PATH Error ',e);
                       res.status(200).send({action: 'REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores '+node.name+ ' remoteMatchingStores Result =', consumerId: notifyConsumerId, notifyPath, status: 404});
                      }
                  
               }  
                break;


            case 'producerNotification' :
                let   notifyPath =[];
                req.body.notifyPath.forEach(function (path) {  notifyPath.push(path); });
                
                sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'Received', data: req.body } });
                let notifyConsumerId = '' // Default Notify return, this store doesnt have the capacity to process this notification
                const existingLocalConsumerSql = 'SELECT * FROM localStoreConsumers WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '"';
                const localCons = alasql(existingLocalConsumerSql);

                // Local Consumer Code Starts Here

                if (localCons.length > 0) {

                   let thisPath = {}; // Add my own store to the path, the producers need to to confirm the notification later
                   if (notifyPath.length > 0)  thisPath = {store:node.name,enforceReversePath:notifyPath[0].enforceReversePath, address:node.listenAddress,port:node.listenPort};
                                          else thisPath = {store:node.name,enforceReversePath:req.body.enforceReversePath|req.body.enforceReversePath, address:node.listenAddress,port:node.listenPort};
                   notifyPath.push(thisPath);
 
                   // If this store has a local consumer on it then on confirmation send out a local notify, note that this store will terminate local service names here and will not forward them
                    
                   const existingNotifySql = 'SELECT * FROM notify WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '" AND srcStoreAddress="' +
                                              req.body.srcStoreAddress + '" AND srcStorePort=' + req.body.srcStorePort + ' AND redlinkMsgId="' + req.body.redlinkMsgId +'"';
                   const existingNotify = alasql(existingNotifySql);

                   // If existing notify, then tell the producer 
                   if (existingNotify && existingNotify.length > 0) {
                       notifyConsumerId = ''; // let this one go, already done.   
                       sendMessage({ debug : { storeName: node.name, action: 'producerNotification', direction: 'LocalConsumerNotifyDeniedAlreadyAccepted', data: req.body } });
                       const enm = calculateEnm(req.body.service,consumer.notifyConsumerId);
                       res.status(200).send({action: 'Return back to producerNotification LocalConsumerNotifyDeniedAlreadyAccepted '+node.name, 
                                            consumerId: notifyConsumerId, redlinkMsgId : req.body.redlinkMsgId, redlinkProducerId : req.body.redlinkProducerId, enm, notifyPath, consumerStore : req.body.storeName, status: 404});
                   }
                 else
                   {
                      // Lets see what the consumers capacity is for service
                      try {
                            sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'LocalConsumerNotifyAccepted', data: req.body } });
                         
                            // OK, now check for my consumer's capacity to actually queue this notify by seeing if the total consumers "inTransitLimit" is in limits. 
                      
                            const consumerIdsSql = 'select consumerId,inTransitLimit from ('+existingLocalConsumerSql+') order by inTransitLimit';
                            const consumerIds = alasql(consumerIdsSql);
                            let enm = 100;
                            let enmHold = enm;

                            // For each consumer on this store, see who can do the job, if none, then reply back to the producer with nothining in the consumerId, stop when a good one found.
                            let notDone = true;
                            consumerIds.forEach(consumer => {
                              enm = calculateEnm(req.body.service,consumer.consumerId);
                              if ( enm < 100 && notifyConsumerId === "" && notDone) { 
                                 // console.log('FINALLY FOUND A LOCAL STORE to INSERT NOTIFY redlinkMsgId=',req.body.redlinkMsgId,' consumerId=',consumer.consumerId, enm);  
                                 notifyConsumerId = consumer.consumerId;
                                 notDone = false;
                                 enmHold = enm;
                               }  
                             else
                               {
                                 // too busy
                                 // notifyConsumerId = ''; // let this one go, already too busy, let the producer send it to someone else.
                                 sendMessage({ debug: { storeName: node.name, action: 'producerNotification', direction: 'LocalConsumerNotifyDeniedinTransitLimitExceeded Enm='+enm, data: req.body } });
                               }
                            }); // forEach loop

                            let resultStatus = 200; 
                            if (notDone) resultStatus = 404;
                            res.status(200).send({action: 'Return back to producerNotification to consumer '+node.name, 
                                                  consumerId: notifyConsumerId, redlinkMsgId : req.body.redlinkMsgId, 
                                                  redlinkProducerId : req.body.redlinkProducerId, 
                                                  enm : enmHold,
                                                  notifyPath, 
                                                  consumerStore : thisPath.store, 
                                                  status: resultStatus});
                          }
                      catch(e) {
                            console.log('ERROR READ ',e);
                            res.status(200).send({action: 'Return back to producerNotification to consumer '+node.name, 
                                                  consumerId: notifyConsumerId, redlinkMsgId : req.body.redlinkMsgId, redlinkProducerId : req.body.redlinkProducerId, 
                                                  enm:100, notifyPath, consumerStore : req.body.storeName, status: 404});
                            }                                
                   }
                   break;
                }

              else
                {
                  // Transit Notify Code starts here
                  // Note that the reverse path state is set in the first notify, so, we can use it to set subsequent notifies
                   let thisPath = {};
                   if (notifyPath.length > 0)  thisPath = {store:node.name,enforceReversePath:notifyPath[0].enforceReversePath, address:node.listenAddress,port:node.listenPort};
                                          else thisPath = {store:node.name,enforceReversePath:req.body.enforceReversePath|req.body.enforceReversePath, address:node.listenAddress,port:node.listenPort};
                   notifyPath.push(thisPath);
                   const remoteMatchingStores = [...new Set([...getRemoteMatchingStores(req.body.service, node.meshName, node.interStoreLoadBalancer)])];
                   
                   try {
                         // This is the interstore notifier
                               req.body.notifyPath = notifyPath;
                               let remoteStore = remoteMatchingStores[0];
                               let destinationStoreAddress = remoteStore.transitStoreAddress;
                               const options = { method: 'POST', url: 'https://' + destinationStoreAddress + '/notify', body: req.body, json: true };
                               //console.log(destinationStoreAddress );
                               async function doStuff() {
                                  const awaitOut = await request(options).then (response => {
                                     if (response.consumerId != "") { 
                                       //Set the ecm value here on response for this store
                                       updateGlobalConsumerEnm(req.body.service, response.consumerId, response.consumerStore, response.enm);
                                     }
                                     res.status(200).send({action: 'REPLY producerNotification forward to stores '+node.name+ ' remoteMatchingStores Result =', 
                                                           consumerId: response.consumerId, redlinkMsgId : response.redlinkMsgId, redlinkProducerId : response.redlinkProducerId, 
                                                           enm:response.enm, notifyPath : response.notifyPath, consumerStore: response.consumerStore, status: response.status});
                                     return response;
                                  }).catch(myError => {
                                  //  console.log(myError);
                                     res.status(200).send({action: 'Error producerNotification forward to stores '+node.name+ ' Connection Reset', status: 404});
                                     return {};
                               });
                               return awaitOut;     
                               }      
                               
                               doStuff(); // This is the actual call to forward the notify , it causes recursive /producerNotifies lookups through the stores.
                         }    
                   catch(e)
                         {
                           res.status(404).send({action: 'REPLY NOTIFY TRANSIT PATH Error producerNotification forward to stores '+node.name+ ' remoteMatchingStores Result =', consumerId: notifyConsumerId, enm:100, notifyPath, status: 404});
                          }
                   break;
                }
        } // case
    }); // notify


    app.post('/read-message', (req, res) => {
        try { // This function allows path recusion backwards to producer
             let notifyPathIn = base64Helper.decode(req.body.notifyPath);
             const notifyPath=notifyPathIn.pop();
             if (notifyPath.address === node.listenAddress && notifyPath.port === node.listenPort)
                {
                }
              else
                {
                  updateGlobalConsumerEnm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.enm);
                  updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay);
                  updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay); // Dont know if the consumer actually read the job, so, set to the same read score.
                  const body = req.body;
                  body.notifyPath = base64Helper.encode(notifyPathIn);
                  const options = { method: 'POST', url: 'https://' + notifyPath.address + ':' + notifyPath.port+ '/read-message', body, json: true  };
                  sendMessage({ debug: { storeName: node.name, action: 'message-read-forwarding', direction: 'relayingBackToProducer', data: options } });
                  request(options, function (error, response) {
                    if (response) {
//                       response.body.status = 200;
                       res.status(200).send(response.body);
                    }
                    else
                    {
//                       response.body.status = 404;                    
                       res.status(200).send(response.body);
                    }   

                });
              }
           }
        catch(e) //todo- fix this- relies on exception to control program flow
           {
             const redlinkMsgId = req.body.redlinkMsgId;
             const redlinkProducerId = req.body.redlinkProducerId;
             const redlinkReadDelay = req.body.readDelay;
             const redlinkConsumerId = req.body.consumerId;
             sendMessage({debug: {storeName: node.name, action: 'read-message', direction: 'inBound', Data: req.body}});
             if (!redlinkMsgId) {
                sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', error: 'redlinkMsgId not specified-400' } });
                res.status(200).send({err: 'redlinkMsgId not specified', redlinkProducerId, status : 400});
                return;
             }
             const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + redlinkMsgId + '" AND read=false AND consumerId ="'+redlinkConsumerId+'"';
             const msgs = alasql(msgSql);// should get one or none
             if (msgs.length > 0) { // will be zero if the message has already been read
                  sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', data: msgs[msgs.length - 1], error: 'none' } });
                  const message = msgs[0];
                  const isLargeMessage = message.isLargeMessage;
                  if (isLargeMessage) {
                     const path = largeMessagesDirectory + redlinkMsgId +'/message.txt';
                     // read msg from path
                     msgs[0].message = fs.readFileSync(path, 'utf-8');
                     fs.removeSync(path);
                  }

                 //This ecm is calculated in and by the consumer, it is the delay between receiving the notify and then performing this read, manual reads drastically increase this time in mS.
                 updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay);
                 updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, 0); //Hasnt replied yet, so, no scrore
 
                 const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + msgs[0].redlinkMsgId + '"';
                 alasql(updateMsgStatus);

                 msgs[0].status = 200;
                 res.send(msgs[0]); // send the oldest message first <<<<<<<<<<<<<<<<<<<<<<<
               
                 if (msgs[0].sendOnly) {            // delete if send only
                    const deleteMsgSql = 'DELETE FROM inMessages WHERE redlinkMsgId="' + redlinkMsgId +'"';
                    const deleteMsg = alasql(deleteMsgSql);
                    updateGlobalConsumerEnm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.enm);
                    NotifyEveryMsgNotRead(); // <------------------------------------------------------------
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
               updateGlobalConsumerEcm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay);
               updateGlobalConsumerErm(req.body.consumerService, req.body.consumerId, req.body.consumerStoreName, req.body.readDelay); // Didnt get to do the job, so, give the reply the same read score.
               const msg = redlinkMsgId ? 'Message with id ' + redlinkMsgId + ' not found- it may have already been read' : 'No unread messages';
               sendMessage({ debug: { storeName: node.name, action: 'read-message', direction: 'outBound', error: msg } });
               res.status(200).send({err:msg, redlinkProducerId, redlinkConsumerId, status : 404});
             }
           }  // catch
    });

    app.post('/reply-message', (req, res) => {
       try { // This function allows path recusion backwards to producer
             let notifyPathIn = base64Helper.decode(req.body.notifyPath);
             const notifyPath=notifyPathIn.pop();
             if (notifyPath.address === node.listenAddress && notifyPath.port === node.listenPort)
               {
               }
             else
               {
                 updateGlobalConsumerEnm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.enm);
                 updateGlobalConsumerErm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.replyDelay); // Update the is score.
                 const body = req.body;
                 body.notifyPath = base64Helper.encode(notifyPathIn);
                 const options = {
                    method: 'POST',
                    url: 'https://' + notifyPath.address + ':' + notifyPath.port+ '/reply-message',
                    body,
                    json: true
                 };
                 sendMessage({ debug: { storeName: node.name, action: 'message-reply-forwarding', direction: 'relayingBackToProducer', data: options } });
                 request(options, function (error, response) {
                   try {
                     res.status(response.statusCode).send(response.body);
                     }
                   catch(e){ //todo error handling and message
                   }
                 });
              }
           }
       catch(e)
           {
             NotifyEveryMsgNotRead(); // <------------------------------------------------------------

             updateGlobalConsumerErm(req.body.replyingService, req.body.replyingServiceId, req.body.replyingStoreName, req.body.replyDelay); // Update the reply score.

             const redlinkMsgId = req.body.redlinkMsgId;
             const redlinkProducerId = req.body.redlinkProducerId;
             // const replyingService = req.body.replyingService;
             const message = req.body.payload;
             var cerror = '';
             if (req.body.cerror) {cerror = req.body.cerror}
             sendMessage({ debug: { storeName: node.name, action: 'reply-message', direction: 'inBound', Data: req.body } });
             const replyMsgSql = 'SELECT DISTINCT * FROM replyMessages WHERE redlinkMsgId="' + redlinkMsgId + '"';
             const replyMsg = alasql(replyMsgSql);
             if (replyMsg.length === 0) {
                let insertReplySql;
               if (isLargeMessage(message)){
                 //store reply to disk
                 const path = largeMessagesDirectory + redlinkMsgId + '/';
                 fs.outputFileSync(path + 'reply.txt', message);
                 insertReplySql = 'INSERT INTO replyMessages ("' + node.name + '","' + redlinkMsgId + '","' + redlinkProducerId + '","' + '' + '", false, true,"'+cerror+'")';
                 }else{
                 insertReplySql = 'INSERT INTO replyMessages ("' + node.name + '","' + redlinkMsgId + '","' + redlinkProducerId + '","' + message + '", false, false,"'+cerror+'")';
               }
               alasql(insertReplySql);
               res.status(200).send({msg: 'Reply received for ', redlinkMsgId: redlinkMsgId}); //todo fix error message
             } else {
               res.status(404).send({msg: 'Reply Rejected for ', redlinkMsgId: redlinkMsgId});
             }
           } //catch
    });


    function deleteOldConsumers() {
      const selectSql = 'select * from globalStoreConsumers where localStoreName = "'+node.name+'" and ttl <= '+ Math.floor(new Date().getTime() / 1000);
      const deleteSql = 'delete   from globalStoreConsumers where localStoreName = "'+node.name+'" and ttl <= '+ Math.floor(new Date().getTime() / 1000);
      const selectData = alasql(selectSql);
      if (selectData && selectData.length > 0) {
        sendMessage({ registration: { storeName: node.name, action: 'deleteConsumer', direction: 'outBound', data: {record:selectData,time:Math.floor(new Date().getTime() / 1000)}, error: 'none' } });
       alasql(deleteSql);
      }
    }

    function reSyncStores(timeOut) {
        return setInterval(function () {
           // First get any local consumers that I own and update my own global entries in my own store, this updates ttl.
           notifyPeerStoreOfLocalConsumers(node.listenAddress, node.listenPort, node.listenAddress, node.listenPort);
           deleteOldConsumers();              // Next  clean up my store first to remove old entries that have not renewed themselves
           notifyAllNorthPeerStoresOnly();    // Make sure the north store knows about me.
        },timeOut);
    }

    function statusStores(timeOut) {
        return setInterval(function () {
           const consumers = getAllVisibleConsumers();
           if (consumers.localConsumers.length > 0) {
             node.status({fill: "green",  shape: "dot", text: 'Local('+consumers.localConsumers.length+') Global('+consumers.globalConsumers.length+')'});
        } else{
             node.status({fill: "yellow", shape: "dot", text: 'Local('+consumers.localConsumers.length+') Global('+consumers.globalConsumers.length+')'});
           }
        },timeOut);
    }

    function isLargeMessage(encodedReplyMessage) {
        return encodedReplyMessage.length > largeMessageThreshold;
    }


    function getAllVisibleServices() {
        const globalConsumersSql = 'SELECT serviceName FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
        const globalConsumers = alasql(globalConsumersSql);
        let   myServices = [];
          globalConsumers.forEach(consumer => {
             myServices.push(consumer.serviceName);
        });
        return myServices;
    }

    function getAllVisibleConsumers() {
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
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
        return { store, messages, notifies, replies };
    }

    node.on("input", msg => {
        log(msg);
        switch (msg.topic) {
            case 'listServices' : {
                sendMessage({command: {services : getAllVisibleServices()}});
                break;
            }
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
        if(node.listenServer){
            node.listenServer.close(() => { //this is asynchronous- callback will be called only when all active connections end https://nodejs.org/api/net.html#net_server_close_callback
                //waiting for all active connection to end may cause node-red to error with 'Error stopping node: Close timed out'
            });
        }
        clearInterval(node.reSyncTimerId);
        clearInterval(node.statusTimerId);
        node.northPeers = config.headers;
        node.southPeers = [];

        //also delete all associated consumers for this store name
        // const dropTriggerNewMsg = 'DROP TRIGGER ' + newMsgTriggerName;
        // alasql(dropTriggerNewMsg);
        dropTrigger(newMsgTriggerName);
        // const dropTriggerRegisterConsumer = 'DROP TRIGGER ' + registerConsumerTriggerName;
        // alasql(dropTriggerRegisterConsumer);
        dropTrigger(registerConsumerTriggerName);

        const remainingMessages = alasql('SELECT * FROM inMessages WHERE storeName="' + node.name + '"');
        remainingMessages.forEach(msg => {
            if(msg.isLargeMessage) {
                const path = largeMessagesDirectory + msg.redlinkMsgId + '/';
                fs.removeSync(path);
            }
        });

        const removeReplySql = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
        const removeNotifySql = 'DELETE FROM notify        WHERE storeName="' + node.name + '"';
        const removeInMessagesSql = 'DELETE FROM inMessages    WHERE storeName="' + node.name + '"';
        alasql(removeReplySql);
        alasql(removeNotifySql);
        alasql(removeInMessagesSql);

        const removeStoreSql = 'DELETE FROM stores WHERE storeName="' + node.name + '"';
        const removeLocalConsumersSql = 'DELETE FROM localStoreConsumers  WHERE storeName="' + node.name + '"';
        const removeGlobalConsumersSql = 'DELETE FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
        alasql(removeStoreSql);
        alasql(removeLocalConsumersSql);
        alasql(removeGlobalConsumersSql);
        done();
    });

    function dropTrigger(triggerName) { //workaround for https://github.com/agershun/alasql/issues/1113
        alasql.fn[triggerName] = () => {
        // console.log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
        }
    }
}; // function

