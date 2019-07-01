module.exports = function (RED) {

    "use strict";
    const alasql = require('alasql');

    const redlinkConsumer = require('./redlinkConsumer.js');
    const redlinkProducer = require('./redlinkProducer.js');
    const redlinkReply = require('./redlinkReply.js');
    const redlinkStore = require('./redlinkStore.js');

    initTables();
    registerNodeRedTypes();
    initNodeRedRoutes();

    function initTables() {
        alasql('DROP TABLE IF EXISTS notify');
        alasql('DROP TABLE IF EXISTS inMessages');
        alasql('DROP TABLE IF EXISTS localStoreConsumers');
        alasql('DROP TABLE IF EXISTS globalStoreConsumers');
        alasql('DROP TABLE IF EXISTS stores');
        alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
        alasql('CREATE TABLE inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)');
        alasql('CREATE TABLE localStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
        alasql('CREATE TABLE globalStoreConsumers (localStoreName STRING, globalServiceName STRING, globalStoreName STRING, globalStoreIp STRING, globalStorePort INT)');
        alasql('CREATE TABLE stores (storeName STRING, storeAddress STRING, storePort INT)'); //todo other fields like listenip/port, north store?
        log('created tables...');
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
        RED.nodes.registerType("redlink reply", redlinkReply.RedLinkReply);
    }

    function getMeshNames() {
        const storesSql = 'SELECT storeName FROM stores';
        const meshStores = alasql(storesSql); //will get a list of mesh:store
        log('in getMeshNames meshStores:', meshStores);
        let meshNames = new Set();
        meshStores.forEach(function (meshStore) {
            const meshStorename = meshStore.storeName;
            const meshName = meshStorename.indexOf(':') != -1 ? meshStorename.substring(0, meshStorename.indexOf(':')) : '';//todo this shouldnt happen
            log('going to push mesh name:', meshName);
            if(meshName){
                meshNames.add(meshName);
            }
        });
        log('returning mesh names:', meshNames)
        return Array.from(meshNames);
    }

    function initNodeRedRoutes() {
        //express routes
        RED.httpAdmin.get("/mesh-names", function (req, res) {
            log('get mesh-names route called...');
            res.json(getMeshNames());
        });
        RED.httpAdmin.get("/store-names", function (req, res) {
            let returnStores = [];
            const mesh = req.query.mesh;
            if(!mesh){
                log('mesh name not specified- going to return empty array in get store anmes route');
                res.json(returnStores);
                return;
            }
            const storesSql = 'SELECT DISTINCT storeName FROM stores WHERE storeName LIKE "'+mesh+'%"'; //console.log(alasql('SELECT * FROM one WHERE a LIKE "abc%"'));
            // console.log(alasql('SELECT * FROM one WHERE a LIKE "'+prefix+'%"'));
            log('\n\n\n\n!@#$%^&*\nstoresSql:', storesSql);
            const stores = alasql(storesSql);
            stores.forEach(meshStore => {
                const meshStorename = meshStore.storeName;
                const storeName = meshStorename.indexOf(':') != -1 ? meshStorename.substring(meshStorename.indexOf(':')+1) : meshStorename;//todo this shouldnt happen
                returnStores.push(storeName);
            });
            res.json(returnStores);
        });
        RED.httpAdmin.get("/consumers", function (req, res) {
            const store = req.query.store;
            let responseJson = getLocalGlobalConsumers(store);
            if (!store) { //shouldnt happen- nothing we can do
                log('no store selected for producer- not populating consumers ');
            }
            res.json(responseJson);
        });
    }

    function getLocalGlobalConsumers(storeName) {
        if (!storeName) {
            return {};
        }
        const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + storeName + '"';
        const localConsumers = alasql(localConsumersSql);
        log('local consumers are:', localConsumers);
        const globalConsumersSql = 'SELECT DISTINCT globalServiceName FROM globalStoreConsumers WHERE localStoreName="' + storeName + '"';//globalStoreConsumers (localStoreName STRING, globalConsumer
        const globalConsumers = alasql(globalConsumersSql);
        log(' global consumers are:', globalConsumers, ' Global consumers are:', globalConsumers);
//        const allConsumers = localConsumers.concat(globalConsumers); //todo filter this for unique consumers
        const allConsumers = globalConsumers; //todo filter this for unique consumers
        log('in get allconsumers going to return', JSON.stringify(allConsumers, null, 2));
        let consumersArray = [];
        allConsumers.forEach(consumer => {
            consumersArray.push(consumer.serviceName || consumer.globalServiceName || consumer.northConsumerName);
        });
        return consumersArray;
    }

    function log() {
        let i = 0;
        let str = '';
        for (; i < arguments.length; i++) {
            str += ' ' + JSON.stringify(arguments[i], null, 2) + ' ';
        }
        // node.trace(str); TODO dont comment this in current state
        console.log(str);
    }

};

