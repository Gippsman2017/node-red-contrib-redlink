module.exports = function (RED) {

    "use strict";
    const httpsServer = require('./https-server.js');
    var alasql = require('alasql');
    var queryString = require('querystring');
    const express = require('express');
    const app = express();

    function RedLinkStore(config) {
        RED.nodes.createNode(this, config);
        this.listenAddress = config.listenAddress;
        this.listenPort = config.listenPort;
        this.peerAddress = config.peerAddress;
        this.peerPort = config.peerPort;
        this.name = config.name;
        this.notifyInterval = config.notifyInterval;
        this.functions = config.functions;
        //todo for testing only... remove this!!!
        try {
            // alasql('DROP TABLE notify');
            alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
        } catch (e) {
            console.log(e);
        }
        console.log('\n\n\n\nthis.listenPort:', this.listenPort);
        if (this.listenPort) {
            this.listenServer = httpsServer.startServer(+this.listenPort, (req, res) => {
                /*
                                notify request is of form:
                                {
                                    type: 'notify',
                                    service: 'abc',
                                    producerIp:'192.168.3.3',
                                    produccerPort:'8000',

                                }
                                //todo register request
                */
                if (req.url.indexOf('?') >= 0) {
                    const reqParams = queryString.parse(req.url.replace(/^.*\?/, ''));
                    if (reqParams.type === 'notify') {
                        const notifyInsertSql = 'INSERT INTO notify VALUES ("' + this.name + '","' + reqParams.service + '","' + reqParams.producerIp + '",' + reqParams.producerPort + ')';
                        console.log('notifyInsertSql:', notifyInsertSql);
                        console.log('Current database 2:', alasql.useid);
                        alasql(notifyInsertSql);
                        const allNotifies = alasql('SELECT * FROM notify');
                        console.log('allNotifies inside da store is:', allNotifies);
                    }
                    console.log(reqParams);
                }
                res.writeHead(200);
                res.end("hello world\n"); //TODO send ACK/NAK
            });
            if (this.listenServer) {
                this.on('close', function (removed, done) {
                    this.listenServer.close(() => {
                        done();
                    });
                })
            }
            console.log('started server at port:', this.listenPort);
        }

        this.on("input", msg => {
            //todo msg can be one of register or notify
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
        const triggerSql = 'CREATE TRIGGER ' + nodeId +
            ' AFTER INSERT ON notify CALL ' + triggerFunctionName + '()';
        console.log('the sql statement for adding trigger in consumer is:', triggerSql);
        alasql(triggerSql);
        console.log('registered trigger for service ', this.name, ' in store ', this.consumerStoreName);
    }

    RED.nodes.registerType("redlink consumer", RedLinkConsumer);

    function RedLinkProducer(config) {
        console.log('producer config:', JSON.stringify(config, null, 2));

        RED.nodes.createNode(this, config);

        var node = this;
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
    })
    RED.httpAdmin.get("/consumers", function (req, res) {
        //TODO get from alasql
        res.json([
            'consumer-1',
            'consumer-2',
            'consumer-3',
            'consumer-4'
        ]);
    });
};
