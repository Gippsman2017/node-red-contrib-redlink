module.exports = function (RED) {

    "use strict";
    const alasql = require('alasql');
    const request = require('request').defaults({strictSSL: false});

    const base64Helper = require('./base64-helper');
    const httpsServer = require('./https-server.js');

    alasql('DROP TABLE IF EXISTS notify');
    alasql('DROP TABLE IF EXISTS inMessages');
    alasql('DROP TABLE IF EXISTS localStoreConsumers');
    alasql('DROP TABLE IF EXISTS southStoreConsumers');
    alasql('DROP TABLE IF EXISTS stores');
    alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
    alasql('CREATE TABLE inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)');
    alasql('CREATE TABLE localStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
    alasql('CREATE TABLE southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
    alasql('CREATE TABLE northStoreConsumers (localStoreName STRING, northConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
    alasql('CREATE TABLE stores (storeName STRING)'); //todo other fields like listenip/port, north store?
    console.log('created tables...');

    function RedLinkStore(config) {
        RED.nodes.createNode(this, config);
        this.listenAddress = config.listenAddress;
        this.listenPort = config.listenPort;
        this.peerAddress = config.peerAddress;
        this.peerPort = config.peerPort;
        this.name = config.name;
        this.notifyInterval = config.notifyInterval;
        this.functions = config.functions;
        this.northPeers = config.headers;
        this.southPeers = null; //todo each store should notify its north peer once when it comes up- thats how southPeers will be populated
        console.log('northPeers:', this.northPeers);
        const node = this;
        const insertStoreSql = 'INSERT INTO stores("' + node.name + '")';
        console.log('in store constructor inserting store name ', node.name, ' in stores table');
        alasql(insertStoreSql); //todo move this to success handler of get store name request above
        // Insert my own store name as a special service, it allows the stores to learn about each other with notifies without having a local consumer registered
// TODO add this back if needed- may not be needed if we store this in the stores table
/*
        const insertMeIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.name + '","#' + node.name + '")';
        alasql(insertMeIntoConsumerSql);
*/

        function notify(ip, port) { //todo this should take ip and port
//TODO send the local consumer list plus south consumers to north/parent store
            //first get distinct local consumers
            console.log('\nin notifyNorth function of ', node.name);
            const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + node.name + '"';
            const localConsumers = alasql(localConsumersSql);
            console.log('local consumers are:', localConsumers);
            const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE localStoreName="' + node.name + '"';//southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
            const southConsumers = alasql(southConsumersSql);
            console.log(' south consumers are:', southConsumers);
            const allConsumers = localConsumers.concat(southConsumers); //todo filter this for unique consumers
            //todo also append northConsumers
            const body = {
                consumers: allConsumers,
                notifyType: 'consumerRegistration',
                southStoreName: node.name,
                southStoreAddress: node.listenAddress,
                southStorePort: node.listenPort
            };
            if (ip !== '0.0.0.0') {
                console.log('going to post to:', 'https://' + ip + ':' + port + '/notify');
                console.log('the body being posted is:', JSON.stringify(body, null, 2));
                const options = {
                    method: 'POST',
                    url: 'https://' + ip + ':' + port + '/notify', //todo add specifier for north or south; also add id (to prevent cyclic notifs)
                    body,
                    json: true
                };
                request(options, function (error, response, body) {
                    if (error) throw new Error(error);
                    console.log(body);
                });
            } else {
                console.log('not posting as peerAddress is not set- this store is probably groot');
            }
        }

        function notifyNorth() {
            node.northPeers.forEach(peer=>{
                notify(peer.ip, peer.port);
            });
        }

        function notifySouth(){ //todo call this whenever notify north is called
            node.southPeers.forEach(peer=>{
                notify(peer.ip, peer.port);
            });
        }

        // alasql('DROP TABLE notify');
        const nodeId = config.id.replace('.', '');
        const newMsgTriggerName = 'onNewMessage' + nodeId;
        const registerConsumerTriggerName = 'registerConsumer' + nodeId;
        try {
            console.log('newMsgTriggerName:', newMsgTriggerName);
            alasql.fn[newMsgTriggerName] = () => {
                //check if the input message is for this store
                //inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
                const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '"';
                console.log('newMessagesSql in consumer:', newMessagesSql);
                var newMessages = alasql(newMessagesSql);
                console.log('newMessages for this store:', newMessages);
                if (newMessages[newMessages.length - 1]) { //insert the last message into notify
                    //TODO add a check- insert into notify only if we have matching consumers here- else send to downstream store (if we have a record that it knows about the consumer)
                    const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessages[newMessages.length - 1].serviceName + '","' + this.listenAddress + '",' + this.listenPort + ')';
                    console.log('in store', node.name, ' going to insert notify new message:', notifyInsertSql);
                    alasql(notifyInsertSql);
                }
                // this.send([newMessages[0], null]);
            };
            alasql.fn[registerConsumerTriggerName] = () => {
                console.log('going to call notifyNorth in consumer trigger of store ', node.name);
                notifyNorth();
            };

            const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName +
                ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
            const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName +
                ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
            console.log('the sql statement for adding triggers in store is:', createNewMsgTriggerSql, '\n', createRegisterConsumerSql);
            try {
                alasql(createNewMsgTriggerSql);
                alasql(createRegisterConsumerSql);
            } catch (e1) {
                console.log('here- problem creating trigger in redlink store...', e1);
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
                    console.log('CONSUMER REGISTRATION');
                    console.log('\n------------------------------------------------------------------------------\nin register consumer route of store ', node.name);
                    console.log("req.body:", req.body);
                    const southStoreName = req.body.southStoreName;
                    const southStoreAddress = req.body.southStoreAddress;
                    const southStorePort = req.body.southStorePort;
                    //delete entries from table before adding them back
                    console.log('\n**************************************\ndropping entries from southConsumers for store name: ', node.name);
                    //************************ Fixed Store issues for south bound multiple stores
                    // const deleteSouthConsumersSql = 'DELETE FROM southStoreConsumers WHERE localStoreName="' + node.name + '" and southStoreName="'+ southStoreName + '"';
                    // alasql(deleteSouthConsumersSql);
                    req.body.consumers.forEach(consumer => {
                        const consumerName = consumer.serviceName || consumer.southConsumerName;
                        const existingSouthConsumerSql = 'SELECT * FROM southStoreConsumers WHERE localStoreName="' + node.name + '" AND southConsumerName="' + consumerName + '"';
                        console.log('existingSouthConsumerSql:', existingSouthConsumerSql);
                        const existingSouthConsumer = alasql(existingSouthConsumerSql);
                        console.log('existingSouthConsumer:', existingSouthConsumer);
                        if (!existingSouthConsumer || existingSouthConsumer.length === 0) {
                            const insertSouthConsumersSql = 'INSERT INTO southStoreConsumers("' + node.name + '","' + consumerName + '","' + southStoreName + '","' + southStoreAddress + '",' + southStorePort + ')';
                            console.log('\n---------------------------------------------------\ninserting into southStoreConsumers sql:', insertSouthConsumersSql);
                            alasql(insertSouthConsumersSql);
                        } else {
                            console.log('not ínserting into south consumers as existingSouthConsumer is:', existingSouthConsumer);
                        }
                    });
                    console.log('\n\ngoing to notify north from  consumer route of store ', node.name);
                    notifyNorth();

                    //southStoreConsumers:(localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
                    //store in table- the consumer name
                    res.send('hello world'); //TODO this will be a NAK/ACK
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

        app.post('/consumer', (req, res) => { //todo validation on params
            console.log('\n\nin register consumer route of store ', node.name);
            console.log("req.body:", req.body);
            const southStoreName = req.body.southStoreName;
            const southStoreAddress = req.body.southStoreAddress;
            const southStorePort = req.body.southStorePort;
            //delete entries from table before adding them back
            console.log('dropping entries from southConsumers for store name: ', node.name);
            const deleteSouthConsumersSql = 'DELETE FROM southStoreConsumers WHERE localStoreName="' + node.name + '" and southStoreName="' + southStoreName + '"';
            alasql(deleteSouthConsumersSql);
            req.body.consumers.forEach(consumer => {
                const consumerName = consumer.serviceName || consumer.southConsumerName;
                const insertSouthConsumersSql = 'INSERT INTO southStoreConsumers("' + node.name + '","' + consumerName + '","' + southStoreName + '","' + southStoreAddress + '",' + southStorePort + ')';
                console.log('inserting into southStoreConsumers sql:', insertSouthConsumersSql);
                alasql(insertSouthConsumersSql);
            });
            console.log('\n\ngoing to notify north from  consumer route of store ', node.name);
            notifyNorth();
            //southStoreConsumers:(localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
            //store in table- the consumer name
            res.send('hello world'); //TODO this will be a NAK/ACK
        });
        this.on("input", msg => {
            if (msg && msg.cmd === 'listConsumers') {
                const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + node.name + '"';
                console.log('onInputMsg localConsumersSql:', localConsumersSql);
                const localConsumers = alasql(localConsumersSql);
                console.log('onInputMsg of ', node.name, ' local consumers are:', localConsumers);
                const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE localStoreName="' + node.name + '"';//southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
                console.log('onInputMsg southConsumersSql:', southConsumersSql);
                const distinctSouthConsumers = alasql(southConsumersSql);
                console.log('onInputMsg of ', node.name, ' distinct south consumers are:', distinctSouthConsumers);
                // const allSouthConsumersforAllStoresSql = 'SELECT * FROM southStoreConsumers'; /*WHERE storeName="' + node.name + '"*/
                // console.log('\n\nallSouthConsumersforAllStoresSql:',alasql(allSouthConsumersforAllStoresSql));
                this.send({localConsumers, southConsumers: distinctSouthConsumers});
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
            const removeSouthConsumersSql = 'DELETE FROM southStoreConsumers WHERE southStoreName="' + node.name + '"';
            console.log('removing south consumers for store name...', node.name);
            alasql(removeSouthConsumersSql);
            //also delete all associated consumers for this store name
            const dropTriggerNewMsg = 'DROP TRIGGER ' + newMsgTriggerName;
            alasql(dropTriggerNewMsg);
            const dropTriggerRegisterConsumer = 'DROP TRIGGER ' + registerConsumerTriggerName;
            alasql(dropTriggerRegisterConsumer);
            done();
        });

    } // function
//------------------------------------------------------- Register this Node --------------------------------
    RED.nodes.registerType("redlink store", RedLinkStore);

    function RedLinkConsumer(config) {
        RED.nodes.createNode(this, config);
        this.name = config.name;
        this.consumerStoreName = config.consumerStoreName;
        const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
        const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;
        console.log('in constructor of consumer:', this.name);
        alasql.fn[newMsgNotifyTrigger] = () => {
            //check if the notify is for this consumer name with the registered store name
            const notifiesSql = 'SELECT * from notify WHERE storeName="' + this.consumerStoreName + '" AND serviceName="' + this.name + '"';
            console.log('notifiesSql in consumer:', notifiesSql);
            var notifies = alasql(notifiesSql);
            console.log('notifies for this consumer:', notifies);
            this.send([notifies[0], null]);
        };
        const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId +
            ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
        console.log('the sql statement for adding trigger in consumer is:', createTriggerSql);
        alasql(createTriggerSql);
        console.log('registered notify trigger for service ', this.name, ' in store ', this.consumerStoreName);
        const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + this.consumerStoreName + '","' + this.name + '")'; //localStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
        console.log('in consumer constructor sql to insert into localStoreConsumer is:', insertIntoConsumerSql);
        alasql(insertIntoConsumerSql);
        console.log('inserted consumer ', this.name, ' for store ', this.consumerStoreName);

        this.on('close', (removed, done) => {
            //todo deregister this consumer
            console.log('in close of consumer...', this.name);
            const dropNotifyTriggerSql = 'DROP TRIGGER ' + msgNotifyTriggerId;
            alasql(dropNotifyTriggerSql);
            console.log('dropped notify trigger...');
            const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + this.consumerStoreName + +'"' + 'AND serviceName="' + this.name + '"';
            alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
            console.log('removed consumer from local store...');
            //TODO use the getlocalAndSouthConsumers function
            const localConsumersSql = 'SELECT * FROM localStoreConsumers';
            const localConsumers = alasql(localConsumersSql);
            console.log('all local consumers are:', localConsumers);
            const southConsumersSql = 'SELECT * FROM southStoreConsumers';
            const southConsumers = alasql(southConsumersSql);
            console.log(' south consumers are:', southConsumers);
            console.log();
            done();
        });
    }

    RED.nodes.registerType("redlink consumer", RedLinkConsumer);

    function RedLinkProducer(config) {

        RED.nodes.createNode(this, config);
        this.producerStoreName = config.producerStoreName;
        this.producerConsumer = config.producerConsumer;
        var node = this;
        node.on("input", msg => {
            msg.msgid = RED.util.generateId();
            const stringify = JSON.stringify(msg);
            const encodedMessage = base64Helper.encode(msg);
            console.log('the input message is:', stringify);
            const msgInsertSql = 'INSERT INTO inMessages VALUES ("' + msg.msgid + '","' + this.producerStoreName + '","' + this.producerConsumer + '","' + encodedMessage + '")';
            console.log('in the consumer going to execute sql to insert into inmesasges: ', msgInsertSql);
            alasql(msgInsertSql);
            const allRows = alasql('select * from inMessages');
            console.log('after inserting input message the inMessages table is:', allRows[0]);
        })
    }

    RED.nodes.registerType("redlink producer", RedLinkProducer);

    function RedLinkReply(config) {
        RED.nodes.createNode(this, config);
        console.log('reply config:', JSON.stringify(config, null, 2));
        var node = this;
    }

    RED.nodes.registerType("redlink reply", RedLinkReply);

    //express routes
    RED.httpAdmin.get("/store-names", function (req, res) {
        const storesSql = 'SELECT DISTINCT storeName FROM stores';
        const stores = alasql(storesSql);
        console.log('\n\n\n\n\n\n\nin RED.httpAdmin.get("/store-names", stores are:', stores);
        let returnStores = [];
        stores.forEach(store => {
            returnStores.push(store.storeName);
        });
        res.json(returnStores);
    });
    RED.httpAdmin.get("/consumers", function (req, res) {
        const producerName = req.query.producer;
        const store = req.query.store;
        let responseJson = getlocalAndSouthConsumers(store);
        if (!store) { //shouldnt happen- nothing we can do
            console.log('no store selected for producer- not populating consumers ');
        }
        res.json(responseJson);
    });

    function getlocalAndSouthConsumers(storeName) {
        if (!storeName) {
            return {};
        }
        const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + storeName + '"';
        const localConsumers = alasql(localConsumersSql);
        console.log('local consumers are:', localConsumers);
        const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE localStoreName="' + storeName + '"';//southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
        const southConsumers = alasql(southConsumersSql);
        console.log(' south consumers are:', southConsumers);
        const allConsumers = localConsumers.concat(southConsumers); //todo filter this for unique consumers
        console.log('in get allconsumers going to return', JSON.stringify(allConsumers, null, 2));
        let consumersArray = [];
        allConsumers.forEach(consumer => {
            consumersArray.push(consumer.serviceName || consumer.southConsumerName);
        });
        return consumersArray;
    }
};
