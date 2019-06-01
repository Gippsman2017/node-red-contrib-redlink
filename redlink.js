module.exports = function (RED) {

    "use strict";
    const alasql = require('alasql');
    const httpsServer = require('./https-server.js');
    const base64Helper = require('./base64-helper');
    alasql('DROP TABLE IF EXISTS notify');
    alasql('DROP TABLE IF EXISTS inMessages');
    alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
    alasql('CREATE TABLE inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)');

    function RedLinkStore(config) {
        RED.nodes.createNode(this, config);
        this.listenAddress = config.listenAddress;
        this.listenPort = config.listenPort;
        this.peerAddress = config.peerAddress;
        this.peerPort = config.peerPort;
        this.name = config.name;
        this.notifyInterval = config.notifyInterval;
        this.functions = config.functions;

        try {
            // alasql('DROP TABLE notify');
            const nodeId = config.id.replace('.', '');
            const triggerFunctionName = 'onNewMessage' + nodeId;
            console.log('triggerFunctionName:', triggerFunctionName);
            alasql.fn[triggerFunctionName] = () => {
                //check if the input message is for this store
                //inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
                const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + this.name +  '"';
                console.log('newMessagesSql in consumer:', newMessagesSql);
                var newMessages = alasql(newMessagesSql);
                console.log('newMessages for this consumer:', newMessages);
                if(newMessages[newMessages.length -1]){ //insert the last message into notify
                    //TODO add a check- insert into notify only if we have matching consumers here- else send to downstream store (if we have a record that it knows about the consumer)
                    const notifyInsertSql = 'INSERT INTO notify VALUES ("' + this.name + '","' + newMessages[newMessages.length -1].serviceName + '","' + this.listenAddress + '",' + this.listenPort + ')';
                    console.log('going to insert notify new message:', notifyInsertSql);
                    alasql(notifyInsertSql);
                }
                // this.send([newMessages[0], null]);
            };
            const triggerSql = 'CREATE TRIGGER ' + nodeId +
                ' AFTER INSERT ON inMessages CALL ' + triggerFunctionName + '()';
            console.log('the sql statement for adding trigger in store is:', triggerSql);
            try {
                alasql(triggerSql);
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
        app.post('/notify', (req, res) =>{ //todo validation on params
            console.log("req.body:", req.body);
            const notifyInsertSql = 'INSERT INTO notify VALUES ("' + this.name + '","' + req.body.service + '","' + req.body.producerIp + '",' + req.body.producerPort + ')';
            console.log('notifyInsertSql:', notifyInsertSql);
            console.log('Current database 2:', alasql.useid);
            alasql(notifyInsertSql);
            const allNotifies = alasql('SELECT * FROM notify');
            console.log('allNotifies inside da store is:', allNotifies);
            res.send('hello world'); //TODO this will be a NAK/ACK
        });
        this.on("input", msg => {
            //todo what messages should we allow? register and notify are handled via endpoints
        });
    } // function
//------------------------------------------------------- Register this Node --------------------------------
    RED.nodes.registerType("redlink store", RedLinkStore);

    function RedLinkConsumer(config) {
        RED.nodes.createNode(this, config);
        this.name = config.name;
        this.consumerStoreName = config.consumerStoreName;
        const nodeId = config.id.replace('.', '');
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
            const dropTriggerSql = 'DROP TRIGGER ' + triggerFunctionName;
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
    }

    RED.nodes.registerType("redlink consumer", RedLinkConsumer);

    function RedLinkProducer(config) {
        console.log('producer config:', JSON.stringify(config, null, 2));

        RED.nodes.createNode(this, config);
        this.producerStoreName = config.producerStoreName;
        this.producerConsumer = config.producerConsumer;
        var node = this;
        node.on("input", msg=>{
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
