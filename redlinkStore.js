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

    node.listenAddress  = config.listenAddress;
    node.listenPort     = config.listenPort;
    node.meshName       = config.meshName;
    node.name           = config.meshName ? config.meshName + ':' + config.name : config.name;
    node.notifyInterval = config.notifyInterval;
    node.functions      = config.functions;
    node.northPeers     = config.headers; //todo validation in ui to prevent multiple norths with same ip:port
    node.southPeers     = []; //todo each store should notify its north peer once when it comes up- that's how southPeers will be populated

    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    alasql(insertStoreSql);
//    const allStoresSql = 'SELECT * FROM stores';

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

    function insertConsumers(body) {
        if (body.globalConsumers) {
            body.globalConsumers.forEach(consumer => {
                const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + consumer.globalServiceName + '"';
                //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
                /*const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' +
                    node.name + '" AND globalServiceName="' + consumer.globalServiceName + '" AND globalStoreIp="'+consumer.globalStoreIp+
                    '" AND globalStorePort='+consumer.globalStorePort;*/

                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' +
                    node.name + '","' + consumer.globalServiceName + '","' + consumer.globalStoreName + '","' + consumer.globalStoreIp + '",' + consumer.globalStorePort + ')';
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) { alasql(insertGlobalConsumersSql); } 
                                                                               else { }
            });
        }
        if (body.localConsumers) {
            body.localConsumers.forEach(consumer => {
                const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + consumer.serviceName + 
                                                  '" AND globalStoreName="' + consumer.storeName + '"';
                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    //get store ip and port
                    const storeDetailsSql = 'SELECT * FROM stores WHERE storeName="' + consumer.storeName + '"';
                    const storeDetails = alasql(storeDetailsSql);
                    if (storeDetails && storeDetails[0]) {
                        const storeAddress = storeDetails[0].storeAddress;
                        const storePort = storeDetails[0].storePort;
                        const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + consumer.serviceName + '","' + consumer.storeName + 
                                                         '","' + storeAddress + '",' + storePort + ')';
                        alasql(insertGlobalConsumersSql);
                    } else { }
                } else { }
            });

        }
    }

    function notifyPeerStoreOfConsumers(ip, port, ipTrail, notifyDirection) {
        if (!ipTrail) {
            ipTrail = [];
        }
        if (ipTrail.includes(ip + ':' + port)) { //loop in notifications- dont send it
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
                localStoreName:     consumer.storeName,
                globalStoreName:    consumer.localStoreName,
                globalServiceName:  consumer.serviceName,
                globalStoreIp:      node.listenAddress,
                globalStorePort:    node.listenPort
            });
        });
        const consumers = getConsumersOfType();
        const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
        if (ip && ip !== '0.0.0.0') {
            const body = getBody(allConsumers, ipTrail, notifyDirection);
            node.send([null,{storeName:node.name, action:'notifyRegistration',direction:'outBound',notifyData:body},null]);
            const options = {
                method: 'POST',
                url:    'https://' + ip + ':' + port + '/notify',
                body,
                json:   true
            };
            request(options, function (error, response) {
                if (error) { node.send([null,{storeName:node.name, action:'notifyRegistrationResult',direction:'outBound',notifyData:body,error:error},null]); } 
                      else { node.send([null,{storeName:node.name, action:'notifyRegistrationResult',direction:'outBound',notifyData:response.body},null]);
                             insertConsumers(response.body);
                           }
                // This is the return message from the post, it contains the north peer consumers, so, we will update our services.
                // node.send({store:node.name,ip:ip,port:port,ipTrail:ipTrail,consumers:body});
            });
        } else { }
    }

    function notifyNorthStoreOfConsumers(northIps) {
        node.northPeers.forEach(peer => { notifyPeerStoreOfConsumers(peer.ip, peer.port, northIps, notifyDirections.NORTH); });
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
        const globalStoresSql = 'SELECT * FROM globalStoreConsumers WHERE globalServiceName="'+serviceName+'" AND globalStoreName LIKE "'+meshName+':%"';
        const matchingGlobalStores = alasql(globalStoresSql);
        const matchingGlobalStoresAddresses = [];
        matchingGlobalStores.forEach(store=>{ matchingGlobalStoresAddresses.push(store.globalStoreIp+':'+store.globalStorePort); });
        return matchingGlobalStoresAddresses;
    }

    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '" AND read='+false;
            var newMessages = alasql(newMessagesSql);
            const newMessage = newMessages[newMessages.length - 1];
            if (newMessage) {
                const allVisibleConsumers = getAllVisibleConsumers(); //todo optimise this
                //insert one notify for local
                for (const localConsumer of allVisibleConsumers.localConsumers) {
                    if(localConsumer.serviceName === newMessage.serviceName){
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessage.serviceName + '","' + node.listenAddress + '",' + node.listenPort + ',"' + newMessage.redlinkMsgId +  '","")';
                        alasql(notifyInsertSql);
                        break; //should get only one local consumer with the same name- this is a just in case
                    }
                }
                //one for each store having a matching global consumer
                const remoteStores = new Set();
                allVisibleConsumers.globalConsumers.forEach(globalConsumer=>{
                    if(globalConsumer.globalServiceName === newMessage.serviceName){
                        remoteStores.add(globalConsumer.globalStoreName);
                    }
                });
                remoteStores.forEach(store=>{
                    //notify (storeName STRING, serviceName STRING, srcStoreIp STRING, srcStorePort INT , redlinkMsgId STRING, notifySent STRING)')
                    const notifyInsertSql = 'INSERT INTO notify VALUES ("' + store + '","' + newMessage.serviceName + '","' + node.listenAddress + '",' + node.listenPort + ',"' + newMessage.redlinkMsgId +  '","")';
                    alasql(notifyInsertSql);
                });

                //remote notify- notify any stores having same consumer not on same node-red instance
                //stores table contains stores local to this node-red instance, all consumers will contain consumers on stores reachable from this store- even if they are remote

                const remoteMatchingStores = getRemoteMatchingStores(newMessage.serviceName, node.meshName);
                remoteMatchingStores.forEach(remoteStore=>{
                    const body = {
                        service:      newMessage.serviceName,
                        srcStoreIp:   node.listenAddress,
                        srcStorePort: node.listenPort,
                        redlinkMsgId: newMessage.redlinkMsgId,
                        notifyType:   'producerNotification'
                    };
                    //'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId +  '")';
                    const options = {
                        method: 'POST',
                        url:    'https://' + remoteStore + '/notify',
                        body,
                        json:   true
                    };
                    request(options, function (error, response) {
                        if (error) {} 
                       else { }
                    });
                });
            }
            //todo notify stores having global consumers
        };
        //On local consumer registration, let them all know
        alasql.fn[registerConsumerTriggerName] = () => { notifyNorthStoreOfConsumers([]); };

        const createNewMsgTriggerSql    = 'CREATE TRIGGER ' + newMsgTriggerName +           ' AFTER INSERT ON inMessages CALL '          + newMsgTriggerName + '()';
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
                node.send([null,{storeName:node.name,action:'notifyConsumerRegistration',direction:'inBound',Data:req.body},null]);
                const storeName = req.body.storeName;
                const storeAddress = req.body.storeAddress;
                const storePort = req.body.storePort;
                const notifyDirection = req.body.notifyDirection;

                //register this as a south store if it is not already in list
                if (notifyDirection === notifyDirections.NORTH && storeAddress && storePort) {
                    if (!node.southPeers.includes(storeAddress + ':' + storePort)) { node.southPeers.push(storeAddress + ':' + storePort); }
                }

                const ips = req.body.ips;
                log('the ips trail is:', ips);

                req.body.consumers.forEach(consumer => {
                    const serviceName = consumer.serviceName || consumer.globalServiceName;
                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + serviceName + '"';
                    //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
                    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                    const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ')';
                    if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                        alasql(insertGlobalConsumersSql);
                    } else {
                    }
                });
                notifyNorthStoreOfConsumers(ips);
                notifySouthStoreOfConsumers(ips);

                const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
                const localConsumers = alasql(localConsumersSql);
                const consumers = getConsumersOfType();
                node.send([null,{storeName:node.name,toStoreName:req.body.storeName,action:'notifyConsumerRegistration',direction:'inBoundReply',Data:{globalConsumers: consumers, localConsumers: localConsumers}},null]);
                res.send({globalConsumers: consumers, localConsumers: localConsumers}); //TODO send back a delta- dont send back consumers just been notified of...
                break;

            case 'producerNotification' :
                node.send([null,null,{storeName:node.name,action:'producerNotification',direction:'inBound',Data:req.body}]);
                const existingLocalConsumerSql = 'SELECT * FROM localStoreConsumers WHERE storeName="' + node.name + '" AND serviceName="' + req.body.service + '"';
                const localCons = alasql(existingLocalConsumerSql);
                if (localCons.length > 0) { 
                   //If this store has a consumer on it then send out a local notify
                   //avoid inserting multiple notifies
                   const existingNotify = alasql('SELECT * FROM notify WHERE storeName="'    + node.name +             '" AND serviceName="' + req.body.service+
                                                                      '" AND srcStoreIp="'   + req.body.srcStoreIp+    '" AND srcStorePort=' + req.body.srcStorePort +
                                                                       ' AND redlinkMsgId="' + req.body.redlinkMsgId + '"');
                   if(!existingNotify || existingNotify.length === 0){
                      const notifyInsertSql = 'INSERT INTO notify VALUES ("'    + node.name + 
                                                                          '","' + req.body.service + '","'     + req.body.srcStoreIp   + 
                                                                          '",'  + req.body.srcStorePort + ',"' + req.body.redlinkMsgId + '","")';
                      alasql(notifyInsertSql);
                      const allNotifies = alasql('SELECT * FROM notify');
                      node.send([null,null,{storeName:node.name,action:'producerNotification',direction:'outBound',Data:allNotifies}]);
                  }
                }
                res.status(200).send('ACK');
                break;
        } //case
    }); // notify

    app.post('/read-message', (req, res)=>{
        const redlinkMsgId = req.body.redlinkMsgId;
        node.send([null,null,{storeName:node.name,action:'read-message',direction:'inBound',Data:req.body}]);
        if(!redlinkMsgId){
            node.send([null,null,{storeName:node.name,action:'read-message',direction:'outBound',error:'redlinkMsgId not specified-400'}]);
            res.status(400).send({err:'redlinkMsgId not specified'});
            return;
        }
        const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="'+redlinkMsgId+'" AND read='+false;
        const msgs = alasql(msgSql);//should get one or none
        if(msgs.length >0){ //will be zero if the message has already been read
            node.send([null,null,{storeName:node.name,action:'read-message',direction:'outBound',Data:msgs[msgs.length-1],error:'none'}]);
            res.send(msgs[msgs.length-1]);
            //update message to read=true
            const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + redlinkMsgId + '"';
            alasql(updateMsgStatus);

        }else{
            node.send([null,null,{storeName:node.name,action:'read-message',direction:'outBound',error:'Message Already Read-404'}]);
            res.status(404).send({error:true,msg:'Message Already Read',redlinkMsgId:redlinkMsgId});
        }
    });

    app.post('/reply-message', (req, res)=>{
        const redlinkMsgId = req.body.redlinkMsgId;
        const redlinkProducerId = req.body.redlinkProducerId;
        // const replyingService = req.body.replyingService;
        const message = req.body.payload;
        const host = req.headers.host;//store address of replying store
        node.send([null,null,{storeName:node.name,action:'reply-message',direction:'inBound',Data:req.body}]);
        const replyMsgSql = 'SELECT DISTINCT * FROM replyMessages WHERE redlinkMsgId="'+redlinkMsgId+'"';
        const replyMsg = alasql(replyMsgSql);
        if (replyMsg.length == 0) {
          const insertReplySql = 'INSERT INTO replyMessages ("'+node.name+'","'+redlinkMsgId+'","'+redlinkProducerId+'","'+message+'", false)';
          const inserteply = alasql(insertReplySql);
        res.status(200).send({msg:'Reply received for ',redlinkMsgId:redlinkMsgId});
        }
        else
        {
        res.status(404).send({msg:'Reply Rejected for ',redlinkMsgId:redlinkMsgId});
        }
    });




    function getAllVisibleConsumers() {
        const localConsumersSql  = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalStoreName<>localStoreName';
        const storesSql          = 'SELECT * FROM stores where storeName ="'+node.name+'"';
        const localConsumers     = alasql(localConsumersSql);
        const globalConsumers    = alasql(globalConsumersSql);
        const store              = alasql(storesSql);
        const peers              = {"northPeers": node.northPeers, "southPeers": node.southPeers}; 
        return {
            localConsumers,
            globalConsumers,
            store,
            peers
        };
    }

   function getCurrentStoreData() {
        const messagesSql = 'SELECT * FROM inMessages    where storeName ="' +node.name+'"'; 
        const notifiesSql = 'SELECT * FROM notify        where storeName ="' +node.name+'"'; 
        const repliesSql  = 'SELECT * FROM replyMessages where storeName ="' +node.name+'"'; 
        const storeSql    = 'SELECT * FROM stores        where storeName ="' +node.name+'"';
        const messages    = alasql(messagesSql);
        const notifies    = alasql(notifiesSql);
        const replies     = alasql(repliesSql);
        const store       = alasql(storeSql);
        return {
            store,
            messages,
            notifies,
            replies
        };    
   }
    node.on("input", msg => {
        log(msg);
        switch(msg.payload) {
           case 'listRegistrations' : { const allConsumers = getAllVisibleConsumers(); node.send(allConsumers); break; };
           case 'listStore'         : { const storeData = getCurrentStoreData(); node.send(storeData); break; };
           case 'flushStore'        : { const removeReplySql      = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
                                        const removeNotifySql     = 'DELETE FROM notify        WHERE storeName="' + node.name + '"';
                                        const removeInMessagesSql = 'DELETE FROM inMessages    WHERE storeName="' + node.name + '"';
                                        alasql(removeReplySql);
                                        alasql(removeNotifySql);
                                        alasql(removeInMessagesSql);
                                        const storeData = getCurrentStoreData(); node.send(storeData); 
                                        break; 
                                        };
        default                     : { node.send({help:"msg.payload can be listRegistrations listStore flushStore"}); break; };
        } 
        //todo what messages should we allow? register and notify are handled via endpoints
    });

    node.on('close', (removed, done) => {
        const removeReplySql      = 'DELETE FROM replyMessages WHERE storeName="' + node.name + '"';
        const removeNotifySql     = 'DELETE FROM notify     WHERE storeName="'    + node.name + '"';
        const removeInMessagesSql = 'DELETE FROM inMessages WHERE storeName="'    + node.name + '"';
        alasql(removeReplySql);
        alasql(removeNotifySql);
        alasql(removeInMessagesSql);

        const removeStoreSql           = 'DELETE FROM stores WHERE storeName="' + node.name + '"';
        const removeDirectConsumersSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.name + '"';
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

