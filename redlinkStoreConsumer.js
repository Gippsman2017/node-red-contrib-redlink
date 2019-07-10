const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});


const notifyDirections = {
    NORTH: 'north',
    SOUTH: 'south'
};

let node;
let expressApp;
let log;
module.exports.init = function(_node, _expressApp, _log){
    node=_node;
    expressApp = _expressApp;
    log = _log;
    log('in redlinkstoreconsumers  init for node:', node.name, 'node.northPeers:', node.northPeers);
    const registerConsumerTriggerName = 'registerConsumer' + node.nodeId;
    try {
        //On local consumer registration, let them all know
        alasql.fn[registerConsumerTriggerName] = () => {
            log('going to call notifyNorth, notifySouth in register consumer trigger of store ', node.name, 'node.northPeers:', node.northPeers);
            notifyNorthStoreOfConsumers([]);
            log('CONSUMER TRIGGER for ', node.name);
        };
        const createRegisterConsumerSql = 'CREATE TRIGGER ' + registerConsumerTriggerName + ' AFTER INSERT ON localStoreConsumers CALL ' + registerConsumerTriggerName + '()';
        alasql(createRegisterConsumerSql);
        log('going to call notifyNorth over here of store ', node.name);
        notifyNorthStoreOfConsumers([]);
        log('CONSUMER TRIGGER for ', node.name);
        notifySouthStoreOfConsumers([]);
    }catch (e1) {
        log('@@@@@@@@@@@@@@@@@@@@here- problem creating trigger in redlink store...', e1);
    }
    node.on('close', (removed, done) => {
        dropTrigger(registerConsumerTriggerName);
        done();
    });

    expressApp.post('/notify', (req, res) => { //todo validation on params
        const notifyType = req.body.notifyType;
        if (notifyType === 'consumerRegistration') {
            log("in notify.consumerRegistration req.body:", req.body);
            const storeName = req.body.storeName;
            const storeAddress = req.body.storeAddress;
            const storePort = req.body.storePort;
            const notifyDirection = req.body.notifyDirection;
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
                //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
                /*
                const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' +
                    node.name + '" AND globalServiceName="' + serviceName + '" AND globalStoreIp="'+storeAddress+
                    '" AND globalStorePort='+storePort;
*/
                //globalStoreConsumers (localStoreName STRING, globalServiceName STRING, globalStoreName STRING, globalStoreIp STRING, globalStorePort INT)');
                const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
                log('EXISTINGGlobalConsumer(', node.name, ':', existingGlobalConsumer);
                const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' + node.name + '","' + serviceName + '","' + storeName + '","' + storeAddress + '",' + storePort + ')';
                if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                    log('SOUTH  inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                    alasql(insertGlobalConsumersSql);
                } else {
                    log('#3 NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
                }
            });
            notifyNorthStoreOfConsumers(ips);
            notifySouthStoreOfConsumers(ips);
            const localConsumersSql = 'SELECT DISTINCT * FROM localStoreConsumers WHERE storeName ="' + node.name + '"';
            const localConsumers = alasql(localConsumersSql);
            const consumers = getConsumers();
            res.send({globalConsumers: consumers, localConsumers: localConsumers});
        } //case
    })

};

function getConsumers() {
    const globalConsumersSql = 'SELECT DISTINCT * FROM globalStoreConsumers WHERE localStoreName="' + node.name + '"';
    return alasql(globalConsumersSql);
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
    const consumers = getConsumers();
    log('ALL CONSUMERS NOTIFY (', node.name, ') ,LOCAL Consumers ARE:', localConsumers, '-', notifyDirection + '-Consumers are at:', consumers, ',', ip, ',', port, ',', ipTrail);
    const allConsumers = qualifiedLocalconsumers.concat(consumers); //todo filter this for unique consumers
    log('allComsumers=', allConsumers);
    //const allConsumers = localConsumers;
    if (ip && ip !== '0.0.0.0') {
        const body = getBody(allConsumers, ipTrail, notifyDirection);
        log('the body being posted is:', JSON.stringify(body, null, 2));
        const options = {
            method: 'POST',
            url: 'https://' + ip + ':' + port + '/notify',
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
        const [ip, port] = peer.split(':');
        log('NOTIFY SOUTH : going to call notifyPeerStoreOfConsumers peer:', ip, '@', port, ' southIps:', southIps);
        notifyPeerStoreOfConsumers(ip, port, southIps, notifyDirections.SOUTH);
    });
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
            //todo need fix for case where remote mesh:store:consumer is same (but ip:port is different)
            /*const existingGlobalConsumerSql = 'SELECT * FROM globalStoreConsumers WHERE localStoreName="' +
                node.name + '" AND globalServiceName="' + consumer.globalServiceName + '" AND globalStoreIp="'+consumer.globalStoreIp+
                '" AND globalStorePort='+consumer.globalStorePort;*/

            const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
            const insertGlobalConsumersSql = 'INSERT INTO globalStoreConsumers("' +
                node.name + '","' + consumer.globalServiceName + '","' + consumer.globalStoreName + '","' + consumer.globalStoreIp + '",' + consumer.globalStorePort + ')';
            if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
                log('Inserting into globalStoreConsumers sql:', insertGlobalConsumersSql);
                alasql(insertGlobalConsumersSql);
            } else {
                log('#1. NOT inserting ', insertGlobalConsumersSql, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
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
                log('#2 NOT inserting ', consumer, ' into global consumers as existingGlobalConsumer is:', existingGlobalConsumer);
            }
        });

    }
}

function dropTrigger(triggerName) { //workaround for https://github.com/agershun/alasql/issues/1113
    alasql.fn[triggerName] = () => {
        log('\n\n\n\nEmpty trigger called for consumer registration', triggerName);
    }
}