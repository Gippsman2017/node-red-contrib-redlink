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
    this.listenAddress = config.listenAddress;
    this.listenPort = config.listenPort;
    this.peerAddress = config.peerAddress;
    this.peerPort = config.peerPort;
    this.name = config.name;
    this.notifyInterval = config.notifyInterval;
    this.functions = config.functions;
    this.northPeers = config.headers; //todo validation in ui to prevent multiple norths with same ip:port
    this.southPeers = []; //todo each store should notify its north peer once when it comes up- that's how southPeers will be populated

    // console.log('northPeers:', this.northPeers);

    const node = this;
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '")';
    console.log('Creating Store and inserting store name ', node.name, ' in stores table');
    alasql(insertStoreSql);

    // const insertMeIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.name + '","#' + node.name + '")';
    // alasql(insertMeIntoConsumerSql);


    function getConsumersOfType(consumerDirection) {
        const globalConsumersSql = 'SELECT DISTINCT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
        return alasql(globalConsumersSql);
/*
        switch (consumerDirection) {
            case notifyDirections.NORTH:
                return alasql(globalConsumersSql);
            case notifyDirections.SOUTH:
                return alasql(globalConsumersSql);
                break;

            default:
                throw new Error('consumerDirection must be specified');

        }
*/
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
                const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalConsumerName="' + consumer.globalConsumerName + '"';
                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' +
                    node.name + '","' + consumer.globalConsumerName + '","' + consumer.globalStoreName + '","' + consumer.globalStoreIp + '",' + consumer.globalStorePort + ')'
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    console.log('Inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                    alasql(insertGlobalConsumersSql);
                } else {
                    console.log('NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                }
            });
        }
    }

    function notifyPeerStoreOfConsumers(ip, port, ipTrail, notifyDirection) {
        if (!ipTrail) {
            ipTrail = [];
        }
        if (ipTrail.includes(ip + ':' + port)) { //loop in notifications- dont send it
            console.log('not notifying as ip:port ', ip + ':' + port, ' is present in ipTrail:', ipTrail);
            return;
        } else {
            ipTrail.push(ip + ':' + port);
            console.log('PUSHING ', ipTrail);
        }
        // first get distinct local consumers
        // console.log('\n >>>>>>>>>>>>>>>>>notifyPeerStoreOfConsumers function of ', node.name,'\n');
        const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const localConsumers = alasql(localConsumersSql);
        let qualifiedLocalconsumers = [];
        localConsumers.forEach(consumer=>{
            qualifiedLocalconsumers.push({
                localStoreName: consumer.storeName,
                globalStoreName: consumer.localStoreName,
                globalConsumerName: consumer.serviceName,
                globalStoreIp: node.listenAddress,
                globalStorePort: node.listenPort
            });
        });
        const consumers = getConsumersOfType(notifyDirection);
        console.log('ALL CONSUMERS NOTIFY (', node.name, ') ,LOCAL Consumers ARE:', localConsumers, '-', notifyDirection + '-Consumers are at:', consumers, ',', ip, ',', port, ',', ipTrail);
        const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
        console.log('allComsumers=', allConsumers);
        //const allConsumers = localConsumers;
        if (ip && ip !== '0.0.0.0') {
            const body = getBody(allConsumers, ipTrail, notifyDirection);
            // console.log('the body being posted is:', JSON.stringify(body, null, 2));
            const options = {
                method: 'POST',
                url: 'https://' + ip + ':' + port + '/notify', //todo add specifier for north or south; also add id (to prevent cyclic notifs)
                body,
                json: true
            };
            request(options, function (error, response) {
                if (error) {
                    console.log('\n\n\n\n\n\n\n\n########\ngot error for request:', options);
                    console.log(error); //todo retry???
                } else {
                    console.log('\n\n\n\n\n\n!@#$%');
                    console.log('sent request to endpoint:\n');
                    console.log(JSON.stringify(options, null, 2));
                    console.log('got response body as:', JSON.stringify(response.body, null, 2));
                    console.log('!@#$%\n\n\n\n\n\n');
                    insertConsumers(response.body);
/*
                    if(response.body){

                    }
*/
                }
                // This is the return message from the post, it contains the north peer consumers, so, we will update our services.
                // node.send({store:node.name,ip:ip,port:port,ipTrail:ipTrail,consumers:body});
            });
        } else {
            // console.log('NOTIFY not posting as peerAddress is not set- this store is probably groot');
        }
    }

    function notifyNorthStoreOfConsumers(northIps) {
        console.log('QWERTY 1 (', node.name, ') North Peers=', node.northPeers, 'NORTHIPs=', northIps, ' peer count=', northIps.length, '\n');
        node.northPeers.forEach(peer => {
            console.log('NOTIFY NORTH : going to call notifyPeerStoreOfConsumers peer:', peer, 'northIps:', northIps);
            notifyPeerStoreOfConsumers(peer.ip, peer.port, northIps, notifyDirections.NORTH);
        });
    }

    function notifySouthStoreOfConsumers(southIps) {
        console.log('QWERTY 1 (', node.name, ') South Peers=', node.southPeers, 'SOUTHIPs=', southIps, ' peer count=', southIps.length, '\n');
        node.southPeers.forEach(peer => {
            var [ip, port] = peer.split(':');
            console.log('NOTIFY SOUTH : going to call notifyPeerStoreOfConsumers peer:', ip, '@', port, ' southIps:', southIps);
            notifyPeerStoreOfConsumers(ip, port, southIps, notifyDirections.SOUTH);
        });
    }

    const nodeId = config.id.replace('.', '');
    const newMsgTriggerName = 'onNewMessage' + nodeId;

    const registerConsumerTriggerName = 'registerConsumer' + nodeId;
    try {
        // console.log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '"';
            // console.log('newMessagesSql in consumer:', newMessagesSql);
            var newMessages = alasql(newMessagesSql);
            // console.log('newMessages for this store:', newMessages);
            if (newMessages[newMessages.length - 1]) { //insert the last message into notify
                //TODO add a check- insert into notify only if we have matching consumers here- else send to downstream store (if we have a record that it knows about the consumer)
                const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessages[newMessages.length - 1].serviceName + '","' + this.listenAddress + '",' + this.listenPort + ')';
                // console.log('in store', node.name, ' going to insert notify new message:', notifyInsertSql);
                alasql(notifyInsertSql);
            }
            // this.send([newMessages[0], null]);
        };
        alasql.fn[registerConsumerTriggerName] = () => {
            // console.log('going to call notifyNorth, notifySouth in consumer trigger of store ', node.name);
            notifyNorthStoreOfConsumers([]);
            // console.log('CONSUMER TRIGGER for ',node.name);
            notifySouthStoreOfConsumers([]);
        };

        const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName + ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
        const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName + ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
        // console.log('the sql statement for adding triggers in store is:', createNewMsgTriggerSql, '\n', createRegisterConsumerSql);
        try {
            alasql(createNewMsgTriggerSql);
            alasql(createRegisterConsumerSql);
            notifyNorthStoreOfConsumers([]);
            // console.log('CONSUMER TRIGGER for ',node.name);
            notifySouthStoreOfConsumers([]);
        } catch (e1) {
            //  console.log('@@@@@@@@@@@@@@@@@@@@here- problem creating trigger in redlink store...', e1);
        }
    } catch (e) {
        console.log(e);
    }
    if (this.listenPort) {
        try {
            this.listenServer = httpsServer.startServer(+this.listenPort);
        } catch (e) {
            console.log('error starting listen server on ', this.listenPort, e);
        }
        if (this.listenServer) {
            this.on('close', (removed, done) => {
                this.listenServer.close(() => {
                    done();
                });
            })
        }
        console.log('started server at port:', this.listenPort);
    }

    const app = httpsServer.getExpressApp();
    app.post('/notify', (req, res) => { //todo validation on params
        const notifyType = req.body.notifyType;
        switch (notifyType) {
            case 'consumerRegistration' :
                console.log("MY req.body:", req.body);
                const storeName = req.body.storeName;
                const storeAddress = req.body.storeAddress;
                const storePort = req.body.storePort;
                const notifyDirection = req.body.notifyDirection;

                //register this as a south store if it is not already in list
                if (notifyDirection === notifyDirections.NORTH && storeAddress && storePort) {
                    if (!node.southPeers.includes(storeAddress + ':' + storePort)) {
                        node.southPeers.push(storeAddress + ':' + storePort);
                        // notify south peer once it has been added so that it is up to date
//                   notifyPeerStoreOfConsumers(storeAddress, storePort, [], notifyDirections.SOUTH);
                    }
                }

                const ips = req.body.ips;
                console.log('the ips trail is:', ips);
                /*if (notifyDirection === notifyDirections.NORTH)*/ {
                    req.body.consumers.forEach(consumer => {
                        const consumerName = consumer.serviceName || consumer.globalConsumerName;
                        const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '" AND globalConsumerName="' + consumerName + '"';
                        const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                        // console.log('EXISTINGGlobalConsumerSql(',node.name,':', existingGlobalConsumer);
                        // console.log('existingGlobalConsumer:', existingGlobalConsumer);
                        const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + consumerName + '","' + storeName + '","' + storeAddress + '",' + storePort + ')';
                        if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                            // console.log('SOUTH  inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                            alasql(insertGlobalConsumersSql);
                        } else {
                            console.log('NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                        }
                    });
                    notifyNorthStoreOfConsumers(ips);
                    notifySouthStoreOfConsumers(ips);
                }

                const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
                const localConsumers = alasql(localConsumersSql);
                const consumers = getConsumersOfType(notifyDirection);
                res.send({globalConsumers: consumers, localConsumers: localConsumers}); //TODO send back a delta- dont send back consumers just been notified of...
                break;

            case 'producerNotification' :
                console.log('PRODUCER NOTIFICATION');
                console.log("req.body:", req.body);
                const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.producerIp + '",' + req.body.producerPort + ')';
                console.log('notifyInsertSql:', notifyInsertSql);
                alasql(notifyInsertSql);
                const allNotifies = alasql('SELECT * FROM notify');
                console.log('allNotifies inside da store is:', allNotifies);
                res.send('hello world'); //TODO this will be a NAK/ACK
                break;
        } //case
    }); // notify


    function getAllConsumers() {
        const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        const globalConsumersSql = 'SELECT DISTINCT * FROM globalStoreConsumers'; /*WHERE globalStoreName<>"' + node.name + '"*/
        const everythingSql = 'SHOW tables';
        const everything = alasql(everythingSql);
        const localConsumers = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        return {
            // everything,
            localConsumers,
            globalConsumers
        };
    }

    this.on("input", msg => {
        console.log(msg);
        if (msg && msg.cmd === 'listConsumers') {
            const allConsumers = getAllConsumers();
            this.send(allConsumers);
        } else if (msg && msg.cmd === 'refreshConsumers') {
            notifyNorthStoreOfConsumers([]);
            this.send({northPeers: this.northPeers, globalPeers: this.southPeers});
        }

        //todo what messages should we allow? register and notify are handled via endpoints
    });

    this.on('close', (removed, done) => { //todo remove triggers
        console.log('on close of store:', node.name, ' going to remove newMsg trigger, register consumer trigger, store name from tables store');
        const removeStoreSql = 'DELETE FROM stores WHERE storeName="' + node.name + '"';
        console.log('removing store name from table stores in store close...', node.name);
        alasql(removeStoreSql);
        const removeDirectConsumersSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.name + '"';
        console.log('removing direct consumers for store name...', node.name);
        alasql(removeDirectConsumersSql);
        const removeGlobalConsumersSql = 'DELETE FROM globalStoreConsumers WHERE globalStoreName="' + node.name + '"';
        console.log('removing global consumers for store name...', node.name);
        alasql(removeGlobalConsumersSql);
        //also delete all associated consumers for this store name
        const dropTriggerNewMsg = 'DROP TRIGGER ' + newMsgTriggerName;
        alasql(dropTriggerNewMsg);
        const dropTriggerRegisterConsumer = 'DROP TRIGGER ' + registerConsumerTriggerName;
        alasql(dropTriggerRegisterConsumer);
        done();
    });
} // function

