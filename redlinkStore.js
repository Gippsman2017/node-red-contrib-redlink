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
    node.peerAddress = config.peerAddress;
    node.peerPort = config.peerPort;
    node.meshName = config.meshName;
    node.name = config.meshName ? config.meshName + ':' + config.name : config.name;
    node.notifyInterval = config.notifyInterval;
    node.functions = config.functions;
    node.northPeers = config.headers; //todo validation in ui to prevent multiple norths with same ip:port
    node.southPeers = []; //todo each store should notify its north peer once when it comes up- that's how southPeers will be populated


    log('northPeers:', node.northPeers);
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    log('Creating Store and inserting store name ', node.name, ' in stores table');
    alasql(insertStoreSql);
    const allStoresSql = 'SELECT * FROM stores';
    log('after inserting store', node.name, ' in constructor the contents fo the stores table is:', alasql(allStoresSql));

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
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    log('Inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                    alasql(insertGlobalConsumersSql);
                } else {
                    log('#1. NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                }
            });
        }
        if (body.localConsumers) {
            body.localConsumers.forEach(consumer => {

                const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + consumer.serviceName + '" AND globalStoreName="' + consumer.storeName + '"';
                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    //get store ip and port
                    const storeDetailsSql = 'SELECT * FROM stores WHERE storeName="' + consumer.storeName + '"';
                    const storeDetails = alasql(storeDetailsSql);
                    log('\n\n\n\n when inserting local consumers the store details:', storeDetails);
                    if (storeDetails && storeDetails[0]) {
                        const storeAddress = storeDetails[0].storeAddress;
                        const storePort = storeDetails[0].storePort;
                        const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' +
                            node.name + '","' + consumer.serviceName + '","' + consumer.storeName + '","' + storeAddress + '",' + storePort + ')';
                        log('Inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                        alasql(insertGlobalConsumersSql);
                    } else {
                        log('!!!!!!!!!! not inserting local consumers from response as store details for ', consumer.storeName, ' could nto be found');
                    }
                } else {
                    log('#2 NOT inserting ', consumer, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                }
            });

        }
    }

    function notifyPeerStoreOfConsumers(ip, port, ipTrail, notifyDirection) {
        if (!ipTrail) {
            ipTrail = [];
        }
        if (ipTrail.includes(ip + ':' + port)) { //loop in notifications- dont send it
            log('not notifying as ip:port ', ip + ':' + port, ' is present in ipTrail:', ipTrail);
            return;
        } else {
            ipTrail.push(ip + ':' + port);
            log('PUSHING ', ipTrail);
        }
        // first get distinct local consumers
        log('\n >>>>>>>>>>>>>>>>>notifyPeerStoreOfConsumers function of ', node.name, '\n');
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const localConsumers = alasql(localConsumersSql);
        let qualifiedLocalconsumers = [];
        localConsumers.forEach(consumer => {
            qualifiedLocalconsumers.push({
                localStoreName: consumer.storeName,
                globalStoreName: consumer.localStoreName,
                globalServiceName: consumer.serviceName,
                globalStoreIp: node.listenAddress,
                globalStorePort: node.listenPort
            });
        });
        const consumers = getConsumersOfType();
        log('ALL CONSUMERS NOTIFY (', node.name, ') ,LOCAL Consumers ARE:', localConsumers, '-', notifyDirection + '-Consumers are at:', consumers, ',', ip, ',', port, ',', ipTrail);
        const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
        log('allComsumers=', allConsumers);
        //const allConsumers = localConsumers;
        if (ip && ip !== '0.0.0.0') {
            const body = getBody(allConsumers, ipTrail, notifyDirection);
            node.send([null,{storeName:node.name, action:'notifyRegistration',direction:'outBound',notifyData:body},null]);
            log('the body being posted is:', JSON.stringify(body, null, 2));
            const options = {
                method: 'POST',
                url: 'https://' + ip + ':' + port + '/notify',
                body,
                json: true
            };
            request(options, function (error, response) {
                if (error) {
                    log('got error for request:', options);
                    log(error); //todo retry???
                    node.send([null,{storeName:node.name, action:'notifyRegistrationResult',direction:'outBound',notifyData:body,error:error},null]);
                } else {
                    log('\n\n\n\n\n\n!@#$%');
                    log('sent request to endpoint:\n');
                    log(JSON.stringify(options, null, 2));
                    log('got response body as:', JSON.stringify(response.body, null, 2));
                    log('!@#$%\n\n\n\n\n\n');
                    node.send([null,{storeName:node.name, action:'notifyRegistrationResult',direction:'outBound',notifyData:response.body},null]);
                    insertConsumers(response.body);
                }
                // This is the return message from the post, it contains the north peer consumers, so, we will update our services.
                // node.send({store:node.name,ip:ip,port:port,ipTrail:ipTrail,consumers:body});
            });
        } else {
            log('NOTIFY not posting as peerAddress is not set- this store is probably root');
        }
    }

    function notifyNorthStoreOfConsumers(northIps) {
        log('notifyNorthStoreOfConsumers (', node.name, ') North Peers=', node.northPeers, 'NORTHIPs=', northIps);
        node.northPeers.forEach(peer => {
            log('NOTIFY NORTH : going to call notifyPeerStoreOfConsumers peer:', peer, 'northIps:', northIps);
            notifyPeerStoreOfConsumers(peer.ip, peer.port, northIps, notifyDirections.NORTH);
        });
    }

    function notifySouthStoreOfConsumers(southIps) {
        log('notifySouthStoreOfConsumers (', node.name, ') South Peers=', node.southPeers, 'SOUTHIPs=', southIps);
        node.southPeers.forEach(peer => {
            var [ip, port] = peer.split(':');
            log('NOTIFY SOUTH : going to call notifyPeerStoreOfConsumers peer:', ip, '@', port, ' southIps:', southIps);
            notifyPeerStoreOfConsumers(ip, port, southIps, notifyDirections.SOUTH);
        });
    }

    const nodeId = config.id.replace('.', '');
    const newMsgTriggerName = 'onNewMessage' + nodeId;

    const registerConsumerTriggerName = 'registerConsumer' + nodeId;

    function getRemoteMatchingStores(serviceName, meshName) {
        //stores (storeName STRING, storeAddress STRING, storePort INT)');
        //globalStoreConsumers (localStoreName STRING, globalServiceName STRING, globalStoreName STRING, globalStoreIp STRING, globalStorePort INT)');
        // log('in getRemoteMatchingStores all global consumers:', alasql('SELECT * FROM globalStoreConsumers'));
        const globalStoresSql = 'SELECT * FROM globalStoreConsumers WHERE globalServiceName="'+serviceName+'" AND globalStoreName LIKE "'+meshName+':%"';
        log('\n in getRemoteMatchingStores \n', 'globalStoresSql:', globalStoresSql);
        const matchingGlobalStores = alasql(globalStoresSql);
        // log('matchingGlobalStores:', matchingGlobalStores);
        const matchingGlobalStoresAddresses = [];
        matchingGlobalStores.forEach(store=>{
            matchingGlobalStoresAddresses.push(store.globalStoreIp+':'+store.globalStorePort);
        });
        log('remote addresses matching service name:', matchingGlobalStoresAddresses);
        return matchingGlobalStoresAddresses;
    }

    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '" AND read='+false;
            log('newMessagesSql in store:', newMessagesSql);
            var newMessages = alasql(newMessagesSql);
            log('newMessages for this store:', newMessages);
            const newMessage = newMessages[newMessages.length - 1];
            if (newMessage) {
                const allVisibleConsumers = getAllVisibleConsumers(); //todo optimise this
                //insert one notify for local
                for (const localConsumer of allVisibleConsumers.localConsumers) {
                    if(localConsumer.serviceName === newMessage.serviceName){
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessage.serviceName + '","' + node.listenAddress + '",' + node.listenPort + ',"' + newMessage.redlinkMsgId +  '","")';
                        log('in store', node.name, ' going to insert notify new message for local consumers:', notifyInsertSql);
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
                    log('in store', node.name, ' going to insert notify new message for global consumers:', notifyInsertSql);
                    alasql(notifyInsertSql);
                });

                //remote notify- notify any stores having same consumer not on same node-red instance
                //stores table contains stores local to this node-red instance, all consumers will contain consumers on stores reachable from this store- even if they are remote

                const remoteMatchingStores = getRemoteMatchingStores(newMessage.serviceName, node.meshName);
                remoteMatchingStores.forEach(remoteStore=>{
                    const body = {
                        service: newMessage.serviceName,
                        srcStoreIp: node.listenAddress,
                        srcStorePort: node.listenPort,
                        redlinkMsgId: newMessage.redlinkMsgId,
                        notifyType: 'producerNotification'
                    };
                    //'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId +  '")';
                    log('the body being posted is:', JSON.stringify(body, null, 2));
                    const options = {
                        method: 'POST',
                        url: 'https://' + remoteStore + '/notify',
                        body,
                        json: true
                    };
                    request(options, function (error, response) {
                        if (error) {
                            log('got error when sending new notify to remote store:', options);
                            log(error); //todo retry???
                        } else {
                            log('\n\n\n\n\n\n!@#$%');
                            log('sent request to endpoint:\n');
                            log(JSON.stringify(options, null, 2));
                            log('got response body as:', JSON.stringify(response.body, null, 2));
                            log('!@#$%\n\n\n\n\n\n');
                        }
                    });
                });
            }
            //todo notify stores having global consumers
        };
        //On local consumer registration, let them all know
        alasql.fn[registerConsumerTriggerName] = () => {
            log('going to call notifyNorth, notifySouth in register consumer trigger of store ', node.name);
            notifyNorthStoreOfConsumers([]);
            log('CONSUMER TRIGGER for ', node.name);
        };

        const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName + ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
        const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName + ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
        log('the sql statement for adding triggers in store is:', createNewMsgTriggerSql, '\n', createRegisterConsumerSql);
        try {
            alasql(createNewMsgTriggerSql);
            alasql(createRegisterConsumerSql);
            log('going to call notifyNorth over here of store ', node.name);
            notifyNorthStoreOfConsumers([]);
            log('CONSUMER TRIGGER for ', node.name);
            notifySouthStoreOfConsumers([]);
        } catch (e1) {
            log('@@@@@@@@@@@@@@@@@@@@here- problem creating trigger in redlink store...', e1);
        }
    } catch (e) {
        log(e);
    }
    if (node.listenPort) {
        try {
            node.listenServer = httpsServer.startServer(+node.listenPort);
        } catch (e) {
            log('error starting listen server on ', node.listenPort, e);
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
                log("in notify.consumerRegistration req.body:", req.body);
                const storeName = req.body.storeName;
                const storeAddress = req.body.storeAddress;
                const storePort = req.body.storePort;
                const notifyDirection = req.body.notifyDirection;

                //register this as a south store if it is not already in list
                if (notifyDirection === notifyDirections.NORTH && storeAddress && storePort) {
                    if (!node.southPeers.includes(storeAddress + ':' + storePort)) {
                        node.southPeers.push(storeAddress + ':' + storePort);
                    }
                }

                const ips = req.body.ips;
                log('the ips trail is:', ips);

                req.body.consumers.forEach(consumer => {
                    const serviceName = consumer.serviceName || consumer.globalServiceName;
                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalServiceName="' + serviceName + '"';
                    //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
                    /*
                    const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' +
                        node.name + '" AND globalServiceName="' + serviceName + '" AND globalStoreIp="'+storeAddress+
                        '" AND globalStorePort='+storePort;
*/
                    //globalStoreConsumers (localStoreName STRING, globalServiceName STRING, globalStoreName STRING, globalStoreIp STRING, globalStorePort INT)');
                    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                    log('EXISTINGGlobalConsumer(', node.name, ':', existingGlobalConsumer);
                    const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ')';
                    if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                        log('SOUTH  inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                        alasql(insertGlobalConsumersSql);
                    } else {
                        log('#3 NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
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
                log('PRODUCER NOTIFICATION');
                log("req.body:", req.body);
                //avoid inserting multiple notifies
                const existingNotify = alasql('SELECT * FROM notify WHERE storeName="'+node.name+'" AND serviceName="'+req.body.service+'" AND srcStoreIp="'+req.body.srcStoreIp+'" AND srcStorePort='+req.body.srcStorePort+' AND redlinkMsgId="'+req.body.redlinkMsgId+'"');
                if(!existingNotify || existingNotify.length === 0){
                    const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId +  '","")';
                    log('notifyInsertSql:', notifyInsertSql);
                    alasql(notifyInsertSql);
                    const allNotifies = alasql('SELECT * FROM notify');
                    node.send([null,null,{storeName:node.name,action:'producerNotification',direction:'outBound',Data:allNotifies}]);
                    //log('allNotifies inside da store is:', allNotifies);
                }
                res.status(200).send('ACK');
                // res.send('hello world'); //TODO this will be a NAK/ACK
                break;
        } //case
    }); // notify
    app.post('/read-message', (req, res)=>{
        log('got a request for read-message in store:', node.name, node.listenAddress, node.listenPort);
        log('the req.body is:', JSON.stringify(req.body, null, 2));
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
            log('updateMsgStatus:', updateMsgStatus);
            alasql(updateMsgStatus);
        }else{
            node.send([null,null,{storeName:node.name,action:'read-message',direction:'outBound',error:'Message Already Read-404'}]);
            res.status(404).send({err:'msg '+redlinkMsgId+' already read'});
        }
    });
    app.post('/reply-message', (req, res)=>{
        log('got a request for reply-message in store:', node.name, node.listenAddress, node.listenPort);
        log('the req.body is:', JSON.stringify(req.body, null, 2));
        const redlinkMsgId = req.body.redlinkMsgId;
        // const replyingService = req.body.replyingService;
        const message = req.body.payload;
        const topic = req.body.topic;
        const host = req.headers.host;//store address of replying store
        log('host:', host);
        const insertReplySql = 'INSERT INTO replyMessages ("'+redlinkMsgId+'","'+message+'", false,"'+topic+'")';
        log('going to insert into reply:', insertReplySql);
        alasql(insertReplySql);
        log('the reply table after inserting is: ', alasql('SELECT * FROM replyMessages'));
        res.status(200).send({msg:'Reply received for '+redlinkMsgId});
    });




    function getAllVisibleConsumers() {
        log('\n\n\n\n\nin getAllVisibleConsumers of ', node.name);
        log('all localStoreConsumers:', alasql('SELECT * FROM localStoreConsumers'));
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalStoreName<>localStoreName';
        const localConsumers = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        return {
            localConsumers,
            globalConsumers
        };
    }

    node.on("input", msg => {
        log(msg);
        if (msg && msg.cmd === 'listConsumers') {
            const allConsumers = getAllVisibleConsumers();
            node.send(allConsumers);
        } else if (msg && msg.cmd === 'listStores') {
            const stores = alasql('SELECT * FROM stores');
            node.send({stores});
        } else if (msg && msg.cmd === 'listInMessages') {
            const messages = alasql('SELECT * FROM inMessages');
            node.send({messages});
        } else if (msg && msg.cmd === 'listNotifies') {
            const notifies = alasql('SELECT * FROM notify');
            node.send({notifies});
        }else if (msg && msg.cmd === 'listPeers') {
            node.send({northPeers: node.northPeers, globalPeers: node.southPeers});
        } else if (msg && msg.cmd === 'listTables') {
            node.send({
                localStoreConsumers: alasql('SELECT * FROM localStoreConsumers'),
                globalStoreConsumers: alasql('SELECT * FROM globalStoreConsumers'),
                stores: alasql('SELECT * FROM stores')
            });
        }
        //todo what messages should we allow? register and notify are handled via endpoints
    });

    node.on('close', (removed, done) => {
        log('on close of store:', node.name, ' going to remove newMsg trigger, register consumer trigger, store name from tables store');
        const removeStoreSql = 'DELETE FROM stores WHERE storeName="' + node.name + '"';
        log('removing store name from table stores in store close...', node.name);
        alasql(removeStoreSql);
        const removeDirectConsumersSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        log('removing direct consumers for store name...', node.name);
        alasql(removeDirectConsumersSql);
        const removeGlobalConsumersSql = 'DELETE FROM globalStoreConsumers WHERE globalStoreName="' + node.name + '"';
        log('removing global consumers for store name...', node.name);
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
            log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
        }
    }
}; // function

