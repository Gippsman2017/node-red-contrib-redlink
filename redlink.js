module.exports = function (RED) {

    "use strict";
    const alasql = require('alasql');
    const os = require('os');

    const redlinkConsumer = require('./redlinkConsumer.js');
    const redlinkProducer = require('./redlinkProducer.js');
    const redlinkStore    = require('./redlinkStore.js');
    const log = require('./log.js')().log; //dont have node yet over here

    initTables();
    registerNodeRedTypes();
    initNodeRedRoutes();

    function initTables() {
        alasql('DROP TABLE IF EXISTS notify');
        alasql('DROP TABLE IF EXISTS inMessages');
        alasql('DROP TABLE IF EXISTS localStoreConsumers');
        alasql('DROP TABLE IF EXISTS globalStoreConsumers');
        alasql('DROP TABLE IF EXISTS stores');
        alasql('DROP TABLE IF EXISTS replyMessages');
        alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, srcStoreAddress STRING, srcStorePort INT , redlinkMsgId STRING, notifySent STRING, read BOOLEAN, redlinkProducerId STRING)');
        alasql('CREATE TABLE inMessages (redlinkMsgId STRING, storeName STRING, serviceName STRING, message STRING, ' +
            'read BOOLEAN, sendOnly BOOLEAN, redlinkProducerId STRING,preserved STRING, timestamp BIGINT, priority INT, ' +
            'isLargeMessage BOOLEAN, lifetime INT, timeSinceNotify INT)');
        alasql('CREATE TABLE localStoreConsumers (storeName STRING, serviceName STRING, consumerId STRING)'); //can have multiple consumers with same name registered to the same store
        alasql('CREATE TABLE globalStoreConsumers (localStoreName STRING, serviceName STRING, consumerId STRING, storeName STRING, direction STRING, storeAddress STRING, storePort INT, transitAddress STRING, transitPort INT, hopCount INT)');
        alasql('CREATE TABLE stores (storeName STRING, storeAddress STRING, storePort INT)');
        alasql('CREATE TABLE replyMessages (storeName STRING, redlinkMsgId STRING, redlinkProducerId STRING, replyMessage STRING, read BOOLEAN, isLargeMessage BOOLEAN)');
        //log('created tables...');
    }

    function registerNodeRedTypes() {
        //Store
        redlinkStore.initRED(RED);
        RED.nodes.registerType("redlink store", redlinkStore.RedLinkStore);
        //Consumer
        redlinkConsumer.initRED(RED);
        RED.nodes.registerType("redlink consumer", redlinkConsumer.RedLinkConsumer);
        //Producer
        redlinkProducer.initRED(RED);
        RED.nodes.registerType("redlink producer", redlinkProducer.RedLinkProducer);
        //Reply
//        redlinkReply.initRED(RED);    RED.nodes.registerType("redlink reply", redlinkReply.RedLinkReply);
    }

    function getMeshNames() {
        const storesSql = 'SELECT storeName FROM stores';
        const meshStores = alasql(storesSql); //will get a list of mesh:store
        let meshNames = new Set();
        meshStores.forEach(function (meshStore) {
            const meshStorename = meshStore.storeName;
            const meshName = meshStorename.indexOf(':') !== -1 ? meshStorename.substring(0, meshStorename.indexOf(':')) : '';//todo this shouldnt happen
            if (meshName) {
                meshNames.add(meshName);
            }
        });
        //log('returning mesh names:', meshNames);
        return Array.from(meshNames);
    }

    function initNodeRedRoutes() {
        //express routes
        RED.httpAdmin.get("/north-peers", (req, res) => { res.json(RED.settings.northPeers || []); });
        RED.httpAdmin.get("/hostname",    (req, res) => { res.json(os.hostname()); });
        RED.httpAdmin.get("/mesh-names",  (req, res) => { res.json(getMeshNames()); });
        RED.httpAdmin.get("/store-names", (req, res) => {
            let returnStores = [];
            const mesh = req.query.mesh;
            if (!mesh) {
                //log('mesh name not specified- going to return empty array in get store names route');
                res.json(returnStores);
                return;
            }
            const storesSql = 'SELECT DISTINCT storeName FROM stores WHERE storeName LIKE "' + mesh + '%"'; //console.log(alasql('SELECT * FROM one WHERE a LIKE "abc%"'));
            const stores = alasql(storesSql);
            stores.forEach(meshStore => {
                const meshStorename = meshStore.storeName;
                const storeName = meshStorename.indexOf(':') !== -1 ? meshStorename.substring(meshStorename.indexOf(':') + 1) : meshStorename;//todo this shouldnt happen
                returnStores.push(storeName);
            });
            res.json(returnStores);
        });

        RED.httpAdmin.get("/all-store-names", (req, res) => { //TODO see if we can use the same route as store-names- maybe pass params?
            let returnStores = [];
            const storesSql = 'SELECT DISTINCT storeName FROM stores'; //console.log(alasql('SELECT * FROM one WHERE a LIKE "abc%"'));
            const stores = alasql(storesSql);
            stores.forEach(meshStore => {
                returnStores.push(meshStore.storeName);
            });
            res.json(returnStores);
        });

        RED.httpAdmin.get("/consumers", (req, res) => {
            const store = req.query.store;
            let responseJson = getLocalGlobalConsumers(store);
            if (!store) { //shouldnt happen- nothing we can do
                //log('no store selected for producer- not populating consumers ');
            }
            res.json(responseJson);
        });
    }

    function getLocalGlobalConsumers(storeName) {
        if (!storeName) {
            return {};
        }
        const meshName = storeName.substring(0, storeName.indexOf(':')); // Producers can only send to Consumers on the same mesh
        const globalConsumers = alasql('SELECT distinct serviceName from ( select * from globalStoreConsumers WHERE localStoreName LIKE "' + meshName + '%"' +
                                                                               ' union select * from localStoreConsumers  WHERE storeName      LIKE "' + meshName + '%") ');
        const allConsumers = [...new Set([...globalConsumers])];
        let consumersArray = [];
        consumersArray.push('msg.topic'); //for dynamically specifying destination consumer- specify in msg.topic
        allConsumers.forEach(consumer => {
            consumersArray.push(consumer.serviceName);
        });
        return consumersArray;
    }
};

