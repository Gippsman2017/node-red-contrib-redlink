module.exports = function (RED) {

    "use strict";
    const alasql = require('alasql');
    const request = require('request').defaults({strictSSL: false});

    const base64Helper = require('./base64-helper');
    const httpsServer = require('./https-server.js');

    alasql('DROP TABLE IF EXISTS notify');
    alasql('DROP TABLE IF EXISTS inMessages');
    alasql('DROP TABLE IF EXISTS currentStoreConsumers');
    alasql('DROP TABLE IF EXISTS southStoreConsumers');
    alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
    alasql('CREATE TABLE inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)');
    alasql('CREATE TABLE currentStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
    alasql('CREATE TABLE southStoreConsumers (currentStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
    console.log('created tables...');

    function RedLinkStore(config) {
        RED.nodes.createNode(this, config);
        console.log('\n\n\n\n redstore config is:', JSON.stringify(config, null, 2));
        this.listenAddress = config.listenAddress;
        this.listenPort = config.listenPort;
        this.peerAddress = config.peerAddress;
        this.peerPort = config.peerPort;
        this.name = config.name;
        this.notifyInterval = config.notifyInterval;
        this.functions = config.functions;
        const node = this;

        function notifyNorth() {
//TODO send the current consumer list plus south consumers to north/parent store
            //first get distinct current consumers
            const currentConsumersSql = 'SELECT DISTINCT serviceName FROM currentStoreConsumers WHERE storeName="' + node.name + '"';
            console.log('in store trigger for new consumers currentConsumersSql:', currentConsumersSql);
            const currentConsumers = alasql(currentConsumersSql);
            console.log('in register consumer trigger of ', node.name, ' current consumers are:', currentConsumers);
            const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE storeName="' + node.name + '"';//southStoreConsumers (currentStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
            console.log('in store trigger for new consumers southConsumersSql:', southConsumersSql);
            const southConsumers = alasql(southConsumersSql);
            console.log('in register consumer trigger of ', node.name, ' south consumers are:', southConsumers);
            const allConsumers = currentConsumers.concat(southConsumers); //todo filter this for unique consumers
            const body = {
                consumers: allConsumers,
                southStoreName: node.name,
                southStoreAddress: node.listenAddress,
                southStorePort: node.listenPort
            };
            if (node.peerAddress !== '0.0.0.0') {
                console.log('going to post to:', 'https://' + node.peerAddress + ':' + node.peerPort + '/consumer');
                console.log('the body being posted is:', JSON.stringify(body, null, 2));
                const options = {
                    method: 'POST',
                    url: 'https://' + node.peerAddress + ':' + node.peerPort + '/consumer',
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

        try {
            // alasql('DROP TABLE notify');
            const nodeId = config.id.replace('.', '');
            const newMsgTriggerName = 'onNewMessage' + nodeId;
            const registerConsumerTriggerName = 'registerConsumer' + nodeId;
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
                notifyNorth();
            };

            try {
                const dropNewMsgTriggerSql = 'DROP TRIGGER ' + newMsgTriggerName;
                console.log('going to drop inMessages trigger in store ' + node.name + ' the sql is:', dropNewMsgTriggerSql);
                alasql(dropNewMsgTriggerSql);
            } catch (e) {
                console.log('error removing inMessages trigger in store...');
            }
            try {
                const dropRegisterConsumerSql = 'DROP TRIGGER ' + registerConsumerTriggerName;
                console.log('going to drop registerConsumer trigger in store ' + node.name + ' the sql is:', dropRegisterConsumerSql);
                alasql(dropRegisterConsumerSql);
            } catch (e) {
                console.log('error removing registerConsumer trigger in store...');
            }

            const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName +
                ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
            const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName +
                ' AFTER INSERT ON currentStoreConsumers CALL ' + registerConsumerTriggerName + '()';
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
        console.log('\n\n\n\nthis.listenPort:', this.listenPort);
        if (this.listenPort) {
            this.listenServer = httpsServer.startServer(+this.listenPort);
            if (this.listenServer) {
                this.on('close', function (removed, done) {
                    this.listenServer.close(() => {
                        done();
                    });
                })
            }
            console.log('started server at port:', this.listenPort);
        }
        const app = httpsServer.getExpressApp();
        app.post('/notify', (req, res) => { //todo validation on params
            console.log("req.body:", req.body);
            const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.producerIp + '",' + req.body.producerPort + ')';
            console.log('notifyInsertSql:', notifyInsertSql);
            alasql(notifyInsertSql);
            const allNotifies = alasql('SELECT * FROM notify');
            console.log('allNotifies inside da store is:', allNotifies);
            res.send('hello world'); //TODO this will be a NAK/ACK
        });
        app.post('/consumer', (req, res) => { //todo validation on params
            console.log('\n\nin register consumer route of north store ', node.name);
            console.log('\n\n\n\n!@#$% the host for this  is:', req.headers.host);
            console.log("req.body:", req.body);
            const southStoreName = req.body.southStoreName;
            const southStoreAddress = req.body.southStoreAddress;
            const southStorePort = req.body.southStorePort;
            req.body.consumers.forEach(consumer => {
                const consumerName = consumer.serviceName;
                const insertSouthConsumersSql = 'INSERT INTO southStoreConsumers("'+node.name+'","'+consumerName+'","'+southStoreName+'","'+southStoreAddress+'",'+southStorePort+')';
                console.log('inserting into southStoreConsumers sql:', insertSouthConsumersSql);
                alasql(insertSouthConsumersSql);
            });
            console.log('\n\ngoing to notify north from  register consumer route of north store ', node.name);
            if(node.name === 'store-2'){ //todo remove- debug only
                console.log('all south consumers:');
                alasql('select * from southStoreConsumers');
            }
            notifyNorth();
            //southStoreConsumers:(currentStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
            //store in table- the consumer name
            res.send('hello world'); //TODO this will be a NAK/ACK
        });
        this.on("input", msg => {
            if (msg && msg.cmd === 'listConsumers') {
                const currentConsumersSql = 'SELECT DISTINCT serviceName FROM currentStoreConsumers WHERE storeName="' + node.name + '"';
                console.log('onInputMsg currentConsumersSql:', currentConsumersSql);
                const currentConsumers = alasql(currentConsumersSql);
                console.log('onInputMsg of ', node.name, ' current consumers are:', currentConsumers);
                const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE currentStoreName="' + node.name + '"';//southStoreConsumers (currentStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
                console.log('onInputMsg southConsumersSql:', southConsumersSql);
                const distinctSouthConsumers = alasql(southConsumersSql);
                console.log('onInputMsg of ', node.name, ' distinct south consumers are:', distinctSouthConsumers);
                // const allSouthConsumersforAllStoresSql = 'SELECT * FROM southStoreConsumers'; /*WHERE storeName="' + node.name + '"*/
                // console.log('\n\nallSouthConsumersforAllStoresSql:',alasql(allSouthConsumersforAllStoresSql));
                this.send({currentConsumers, southConsumers: distinctSouthConsumers});
            }
            //todo what messages should we allow? register and notify are handled via endpoints
        });
    } // function
//------------------------------------------------------- Register this Node --------------------------------
    RED.nodes.registerType("redlink store", RedLinkStore);

    function RedLinkConsumer(config) {
        RED.nodes.createNode(this, config);
        console.log('\n\n\n\n redstore consumer is:', JSON.stringify(config, null, 2));
        this.name = config.name;
        this.consumerStoreName = config.consumerStoreName;
        const nodeId = 'a' + config.id.replace('.', '');
        const triggerFunctionName = 'onNotify' + nodeId;
        console.log('triggerFunctionName:', triggerFunctionName);
        alasql.fn[triggerFunctionName] = () => {
            //check if the notify is for this consumer name with the registered store name
            const notifiesSql = 'SELECT * from notify WHERE storeName="' + this.consumerStoreName + '" AND serviceName="' + this.name + '"';
            console.log('notifiesSql in consumer:', notifiesSql);
            var notifies = alasql(notifiesSql);
            console.log('notifies for this consumer:', notifies);
            this.send([notifies[0], null]);
        };
        try {
            const dropTriggerSql = 'DROP TRIGGER ' + nodeId;
            console.log('going to drop notify trigger in consumer' + this.name + 'store name:', this.consumerStoreName, 'the sql is:', dropTriggerSql);
            console.log('when dropping notify trigger in consumer alasql returns:', alasql(dropTriggerSql));
        } catch (e) {
            console.log('error removing trigger in consumer...');
        }
        const createTriggerSql = 'CREATE TRIGGER ' + nodeId +
            ' AFTER INSERT ON notify CALL ' + triggerFunctionName + '()';
        console.log('the sql statement for adding trigger in consumer is:', createTriggerSql);
        alasql(createTriggerSql);
        console.log('registered trigger for service ', this.name, ' in store ', this.consumerStoreName);
        const insertIntoConsumerSql = 'INSERT INTO currentStoreConsumers ("' + this.consumerStoreName + '","' + this.name + '")'; //currentStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
        console.log('in consumer constructor sql to insert into currentStoreConsumer is:', insertIntoConsumerSql);
        alasql(insertIntoConsumerSql);
        console.log('inserted service name ', this.name, ' with store ', this.consumerStoreName);

        this.on("close", () => {
            console.log('in ')
            const insertIntoConsumerSql = 'INSERT INTO currentStoreConsumers ' + this.consumerStoreName + ' ' + this.name; //currentStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
            alasql(insertIntoConsumerSql);
        });
    }

    RED.nodes.registerType("redlink consumer", RedLinkConsumer);

    function RedLinkProducer(config) {
        console.log('producer config:', JSON.stringify(config, null, 2));

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
        //TODO get from alasql
        res.json([
            'store-1',
            'store-2',
            'store-3',
            'store-4'
        ]);
    });
    RED.httpAdmin.put("/store-names", function (req, res) {
    });
    RED.httpAdmin.get("/consumers", function (req, res) {
        //TODO get from alasql
        res.json([
            'wombat1',
            'wombat2',
        ]);
    });
};
