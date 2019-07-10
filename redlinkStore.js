const alasql = require('alasql');
const httpsServer = require('./https-server.js');
const storeConsumer = require('./redlinkStoreConsumer.js');
let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkStore = function (config) {
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
    node.northPeers = config.headers||[]; //todo validation in ui to prevent multiple norths with same ip:port
    node.southPeers = []; //todo each store should notify its north peer once when it comes up- that's how southPeers will be populated
    node.nodeId = config.id.replace('.', '');
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
    storeConsumer.init(node, app, log);
    log('northPeers:', node.northPeers);
    const insertStoreSql = 'INSERT INTO stores("' + node.name + '","' + node.listenAddress + '",' + node.listenPort + ')';
    log('Creating Store and inserting store name ', node.name, ' in stores table');
    alasql(insertStoreSql);
    const allStoresSql = 'SELECT * FROM stores';
    log('after inserting store', node.name, ' in constructor the contents fo the stores table is:', alasql(allStoresSql));

    const newMsgTriggerName = 'onNewMessage' + node.nodeId;

    function getRemoteMatchingStores(serviceName, meshStore, address, port) {
        //stores (storeName STRING, storeAddress STRING, storePort INT)');
        //globalStoreConsumers (localStoreName STRING, globalServiceName STRING, globalStoreName STRING, globalStoreIp STRING, globalStorePort INT)');
        const globalStoresSql = 'SELECT * FROM globalStoreConsumers WHERE globalServiceName="'+serviceName+'" AND globalStoreName="'+meshStore+'"';
        log('\n in getRemoteMatchingStores \n', 'globalStoresSql:', globalStoresSql);
        const matchingGlobalStores = alasql(globalStoresSql);
        log('matchingGlobalStores:', matchingGlobalStores);
        let allLocalStoresSql = 'SELECT * FROM stores';
        const allLocalStores = alasql(allLocalStoresSql);
        log(allLocalStores);
        const matchingGlobalStoresAddresses = [];
        matchingGlobalStores.forEach(store=>{
            matchingGlobalStoresAddresses.push(store.globalStoreIp+':'+store.globalStorePort);
        });
        const localStoresAddresses = [];
        allLocalStores.forEach(store=>{
            localStoresAddresses.push(store.storeAddress+':'+store.storePort);
        });
        const remoteStoreAddresses= [];
        matchingGlobalStoresAddresses.forEach(remoteAddress=>{
            if(!localStoresAddresses.includes(remoteAddress)){
                remoteStoreAddresses.push(remoteAddress);
            }
        });
        log('remote addresses matching service name:', remoteStoreAddresses);
        return remoteStoreAddresses;
    }

    try {
        log('newMsgTriggerName:', newMsgTriggerName);
        alasql.fn[newMsgTriggerName] = () => {
            // check if the input message is for this store
            // inMessages (msgId STRING, storeName STRING, serviceName STRING, message STRING)'
            const newMessagesSql = 'SELECT * from inMessages WHERE storeName="' + node.name + '"'; //todo also filter by address and port? what happens if we have multiple non-unique mesh:store?
            log('newMessagesSql in consumer:', newMessagesSql);
            var newMessages = alasql(newMessagesSql);
            log('newMessages for this store:', newMessages);
            const newMessage = newMessages[newMessages.length - 1];
            if (newMessage) {
                //insert the last message into notify
                //local notify- store src=store dest
                const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + newMessage.serviceName + '","' + node.listenAddress + '",' + node.listenPort + ',"' + newMessage.redlinkMsgId +  '","")';
                log('in store', node.name, ' going to insert notify new message:', notifyInsertSql);
                alasql(notifyInsertSql);

                //remote notify- notify any stores having same consumer not on same node-red instance
                //stores table contains stores local to this node-red instance, all consumers will contain consumers on stores reachable from this store- even if they are remote
                getRemoteMatchingStores(newMessage.serviceName, node.name,node.listenAddress, node.listenPort);//todo fix bug on 61, 298; then do notify
                //we need to add a local notify stores not on this instance but having services which can handle this message

            }
            //todo notify stores having global consumers
        };


        const createNewMsgTriggerSql = 'CREATE TRIGGER ' + newMsgTriggerName + ' AFTER INSERT ON inMessages CALL ' + newMsgTriggerName + '()';
        log('the sql statement for adding triggers in store is:', createNewMsgTriggerSql);
        try {
            alasql(createNewMsgTriggerSql);
        } catch (e1) {
            log('@@@@@@@@@@@@@@@@@@@@here- problem creating trigger in redlink store...', e1);
        }
    } catch (e) {
        log(e);
    }
    app.post('/notify', (req, res) => { //todo validation on params
        const notifyType = req.body.notifyType;
        if (notifyType === 'producerNotification') {
            log('PRODUCER NOTIFICATION');
            log("req.body:", req.body);
            const notifyInsertSql = 'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.producerIp + '",' + req.body.producerPort + ',"")';
            log('notifyInsertSql:', notifyInsertSql);
            alasql(notifyInsertSql);
            const allNotifies = alasql('SELECT * FROM notify');
            log('allNotifies inside da store is:', allNotifies);
            res.send('hello world');
        } //case
    }); // notify
    app.post('/read-message', (req, res)=>{
        log('got a request for read-message in store:', node.name, node.listenAddress, node.listenPort);
        log('the req.body is:', JSON.stringify(req.body, null, 2));
        const redlinkMsgId = req.body.redlinkMsgId;
        if(!redlinkMsgId){
            res.status(400).send({err:'redlinkMsgId not specified'});
            return;
        }
        const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="'+redlinkMsgId+'" AND read='+false;
        const msgs = alasql(msgSql);//should get one or none
        if(msgs.length >0){ //will be zero if the message has already been read
            res.send(msgs[msgs.length-1]);
            //update message to read=true
            const updateMsgStatus = 'UPDATE inMessages SET read=' + true + ' WHERE redlinkMsgId="' + redlinkMsgId + '"';
            log('updateMsgStatus:', updateMsgStatus);
            alasql(updateMsgStatus);
        }else{
            res.status(404).send({err:'msg '+redlinkMsgId+' already read'});
        }
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
        } else if (msg && msg.cmd === 'listPeers') {
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
        done();
    });

    function dropTrigger(triggerName) { //workaround for https://github.com/agershun/alasql/issues/1113
        alasql.fn[triggerName] = () => {
            log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
        }
    }
}; // function

