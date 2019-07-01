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
                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' +
                    node.name + '","' + consumer.globalServiceName + '","' + consumer.globalStoreName + '","' + consumer.globalStoreIp + '",' + consumer.globalStorePort + ')';
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    log('Inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                    alasql(insertGlobalConsumersSql);
                } else {
                    log('NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
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
                    log('NOT inserting ', consumer, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
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
            log('the body being posted is:', JSON.stringify(body, null, 2));
            const options = {
                method: 'POST',
                url: 'https://' + ip + ':' + port + '/notify', //todo add specifier for north or south; also add id (to prevent cyclic notifs)
                body,
                json: true
            };
            request(options, function (error, response) {
                if (error) {
                    log('got error for request:', options);
                    log(error); //todo retry???
                } else {
                    log('\n\n\n\n\n\n!@#$%');
                    log('sent request to endpoint:\n');
                    log(JSON.stringify(options, null, 2));
                    log('got response body as:', JSON.stringify(response.body, null, 2));
                    log('!@#$%\n\n\n\n\n\n');
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
    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '"';
            log('newMessagesSql in consumer:', newMessagesSql);
            var newMessages = alasql(newMessagesSql);
            log('newMessages for this store:', newMessages);
            if (newMessages[newMessages.length - 1]) { //insert the last message into notify
                //TODO add a check- insert into notify only if we have matching consumers here- else send to downstream store (if we have a record that it knows about the consumer)
                const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessages[newMessages.length - 1].serviceName + '","' + node.listenAddress + '",' + node.listenPort + ')';
                log('in store', node.name, ' going to insert notify new message:', notifyInsertSql);
                alasql(notifyInsertSql);
            }
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
                    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                    log('EXISTINGGlobalConsumer(', node.name, ':', existingGlobalConsumer);
                    const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ')';
                    if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                        log('SOUTH  inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                        alasql(insertGlobalConsumersSql);
                    } else {
                        log('NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                    }
                });
                notifyNorthStoreOfConsumers(ips);
                notifySouthStoreOfConsumers(ips);

                const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
                const localConsumers = alasql(localConsumersSql);
                const consumers = getConsumersOfType();
                res.send({globalConsumers: consumers, localConsumers: localConsumers}); //TODO send back a delta- dont send back consumers just been notified of...
                break;

            case 'producerNotification' :
                log('PRODUCER NOTIFICATION');
                log("req.body:", req.body);
                const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.producerIp + '",' + req.body.producerPort + ')';
                log('notifyInsertSql:', notifyInsertSql);
                alasql(notifyInsertSql);
                const allNotifies = alasql('SELECT * FROM notify');
                log('allNotifies inside da store is:', allNotifies);
                res.send('hello world'); //TODO this will be a NAK/ACK
                break;
        } //case
    }); // notify


    function getAllVisibleConsumers() {
        log('\n\n\n\n\nin getAllVisibleConsumers of ', node.name);
        log('all localStoreConsumers:', alasql('SELECT * FROM localStoreConsumers'));
        const localConsumersSql  = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalStoreName<>localStoreName';

/*
        const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        log('localConsumersSql:', localConsumersSql);
        log('all localStoreConsumers:', alasql('SELECT * FROM localStoreConsumers'));
*/


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
        } else if (msg && msg.cmd === 'listPeers') {
            node.send({northPeers: node.northPeers, globalPeers: node.southPeers});
        } else if (msg && msg.cmd === 'listTables') {
            node.send({
                localStoreConsumers:  alasql('SELECT * FROM localStoreConsumers'),
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


    function log() {
        let i = 0;
        let str = '';
        for (; i < arguments.length; i++) {
            str += ' ' + JSON.stringify(arguments[i], null, 2) + ' ';
        }
        node.trace(str);
    }
}; // function

