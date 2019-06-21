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
        alasql('DROP TABLE IF EXISTS southStoreConsumers');
        alasql('DROP TABLE IF EXISTS stores');
        alasql('CREATE TABLE notify (storeName STRING, serviceName STRING, producerIp STRING, producerPort INT )');
        alasql('CREATE TABLE inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)');
        alasql('CREATE TABLE localStoreConsumers (storeName STRING, serviceName STRING)'); //can have multiple consumers with same name registered to the same store
        alasql('CREATE TABLE southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
        alasql('CREATE TABLE northStoreConsumers (localStoreName STRING, northConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)');
        alasql('CREATE TABLE stores (storeName STRING)'); //todo other fields like listenip/port, north store?
        console.log('created tables...');
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

    function initNodeRedRoutes() {
        //express routes
        RED.httpAdmin.get("/store-names", function (req, res) {
            const storesSql = 'SELECT DISTINCT storeName FROM stores';
            const stores = alasql(storesSql);
            let returnStores = [];
            stores.forEach(store => {
                returnStores.push(store.storeName);
            });
            res.json(returnStores);
        });
        RED.httpAdmin.get("/consumers", function (req, res) {
            const producerName = req.query.producer;
            const store = req.query.store;
            let responseJson = getlocalNorthSouthConsumers(store);
            if (!store) { //shouldnt happen- nothing we can do
                console.log('no store selected for producer- not populating consumers ');
            }
            res.json(responseJson);
        });
    }

    function getlocalNorthSouthConsumers(storeName) {
        if (!storeName) {
            return {};
        }
        const localConsumersSql = 'SELECT DISTINCT serviceName FROM localStoreConsumers WHERE storeName="' + storeName + '"';
        const localConsumers = alasql(localConsumersSql);
        console.log('local consumers are:', localConsumers);
        const southConsumersSql = 'SELECT DISTINCT southConsumerName FROM southStoreConsumers WHERE localStoreName="' + storeName + '"';//southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
        const southConsumers = alasql(southConsumersSql);
        const northConsumersSql = 'SELECT DISTINCT northConsumerName FROM northStoreConsumers WHERE localStoreName="' + storeName + '"';//southStoreConsumers (localStoreName STRING, southConsumerName STRING, southStoreName STRING, southStoreIp STRING, southStorePort INT)')
        const northConsumers = alasql(northConsumersSql);
        console.log(' south consumers are:', southConsumers, ' north consumers are:', northConsumers);
        const allConsumers = localConsumers.concat(southConsumers).concat(northConsumers); //todo filter this for unique consumers
        console.log('in get allconsumers going to return', JSON.stringify(allConsumers, null, 2));
        let consumersArray = [];
        allConsumers.forEach(consumer => {
            consumersArray.push(consumer.serviceName || consumer.southConsumerName || consumer.northConsumerName);
        });
        return consumersArray;
    }
};
