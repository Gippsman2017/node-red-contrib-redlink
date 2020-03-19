const alasql = require('alasql');
const fs = require('fs-extra');

module.exports.calculateEnm = function(serviceName, consumerId, storeName) {
    const existingLocalConsumerSql = `SELECT * FROM localStoreConsumers   WHERE storeName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}"`;
    const notifyCountSql = `select count(*) nc from (SELECT * FROM notify WHERE storeName="${storeName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}")`;
    const nc = alasql(notifyCountSql)[0].nc;
    const itlSql = `select inTransitLimit from (${existingLocalConsumerSql})`;
    const itl = alasql(itlSql)[0].inTransitLimit;
    return ((nc) / itl * 100).toFixed(0);
};

module.exports.insertGlobalConsumer = function(serviceName, consumerId, storeName, direction, storeAddress, storePort, transitAddress, transitPort, hopCount, ttl, ecm, erm, enm, localStoreName) {
    const existingGlobalConsumerSql = `SELECT * FROM globalStoreConsumers WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}" AND storeAddress = "${storeAddress}" AND storePort = ${storePort}`;
    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
    const insertGlobalConsumersSql = `INSERT INTO globalStoreConsumers("${localStoreName}","${serviceName}","${consumerId}","${storeName}","${direction}","${storeAddress}",${storePort},"${transitAddress}",${transitPort},${hopCount},${ttl},${ecm},${erm},${enm})`;
    if (!existingGlobalConsumer || existingGlobalConsumer.length === 0) {
        alasql(insertGlobalConsumersSql);
    } 
  else 
    {
        alasql(`UPDATE globalStoreConsumers SET ttl=${ttl}  WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND consumerId="${consumerId}" AND storeName="${storeName}" AND storeAddress = "${storeAddress}" AND storePort = ${storePort}`);
        // Possibly Need to add a delete and an insert here for lower hopCount routes, it will reduce the number of notifies on high complexity redlink store layouts.
    }
};

module.exports.sendMessage = function(msg, node) { // command, registration, debug //todo add error port?
    const msgs = [];
    if (node.command) { msgs.push(msg.command); }
    if (node.registration) { msgs.push(msg.registration); }
    if (node.debug) { msgs.push(msg.debug); }
    node.send(msgs);
};

module.exports.updateGlobalConsumerEcm = function(serviceName, consumerId, storeName, ecm, localStoreName) {
    alasql(`UPDATE globalStoreConsumers SET ecm=${ecm} WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND storeName="${storeName}" AND consumerId="${consumerId}"`);
};

module.exports.updateGlobalConsumerErm = function(serviceName, consumerId, storeName, erm, localStoreName) {
    const existingGlobalConsumerSql = `SELECT * FROM globalStoreConsumers WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND storeName="${storeName}"`;
    const existingGlobalConsumer = alasql(existingGlobalConsumerSql);
    if (existingGlobalConsumer) {
        const updateConsumerErm = `UPDATE globalStoreConsumers SET erm=${erm} WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND storeName="${storeName}"AND consumerId="${consumerId}"`;
        alasql(updateConsumerErm);
    }
};

module.exports.updateGlobalConsumerEnm = function(serviceName, consumerId, storeName, enm, localStoreName) {
    alasql(`UPDATE globalStoreConsumers SET enm=${enm} WHERE localStoreName="${localStoreName}" AND serviceName="${serviceName}" AND storeName="${storeName}" AND consumerId="${consumerId}"`);
};

module.exports.closeStore = function(node, done) {
    if (node.listenServer) {
        node.listenServer.close(() => { //this is asynchronous- callback will be called only when all active connections end https://nodejs.org/api/net.html#net_server_close_callback
            //waiting for all active connection to end may cause node-red to error with 'Error stopping node: Close timed out'
        });
    }
    clearInterval(node.reSyncTimerId);
    clearInterval(node.statusTimerId);
    node.northPeers = [];
    node.southPeers = [];
    //also delete all associated consumers for this store name
    dropTrigger(node.newMsgTriggerName);
    dropTrigger(node.registerConsumerTriggerName);
    const remainingMessages = alasql(`SELECT * FROM inMessages WHERE storeName="${node.name}"`);
    remainingMessages.forEach(msg => {
        if (msg.isLargeMessage) {
            const path = node.largeMessagesDirectory + msg.redlinkMsgId + '/';
            fs.remove(path);
        }
    });
    const removeReplySql = `DELETE FROM replyMessages WHERE storeName="${node.name}"`;
    const removeNotifySql = `DELETE FROM notify WHERE storeName="${node.name}"`;
    const removeInMessagesSql = `DELETE FROM inMessages WHERE storeName="${node.name}"`;
    alasql(removeReplySql);
    alasql(removeNotifySql);
    alasql(removeInMessagesSql);
    const removeStoreSql = `DELETE FROM stores WHERE storeName="${node.name}"`;
    const removeLocalConsumersSql = `DELETE FROM localStoreConsumers WHERE storeName="${node.name}"`;
    const removeGlobalConsumersSql = `DELETE FROM globalStoreConsumers WHERE localStoreName="${node.name}"`;
    alasql(removeStoreSql);
    alasql(removeLocalConsumersSql);
    alasql(removeGlobalConsumersSql);
    done();
};

function dropTrigger (triggerName) {
    alasql(`DROP TRIGGER ${triggerName}`);
}

