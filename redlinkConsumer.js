const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});

const base64Helper = require('./base64-helper.js');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkConsumer = function (config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const log  = require('./log.js')(node).log;
    node.name  = config.name;
    node.consumerStoreName = config.consumerStoreName;
    node.consumerMeshName = config.consumerMeshName;
    node.manualRead = config.manualReadReceiveSend;
    if (node.consumerMeshName) { node.consumerStoreName = node.consumerMeshName + ':' + node.consumerStoreName; } 
                          else { }
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;

    alasql.fn[newMsgNotifyTrigger] = () => {
        //check if the notify is for this consumer name with the registered store name
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' +
                                                                       node.name + '"' + ' AND notifySent NOT LIKE "%' + node.id + '%"';
        const notifies = alasql(notifiesSql);
        const newNotify = notifies[notifies.length - 1];
        if (!newNotify) {
            return; //nothing to do- trigger for some other service
        }
        const existingNotifiedNodes = newNotify.notifySent.trim();
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const updateNotifySql = 'UPDATE notify SET notifySent="' + newNotifiedNodes + '" WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND storeName="' + node.consumerStoreName + '"';
        alasql(updateNotifySql);
        const notifyMessage = {
            redlinkMsgId: newNotify.redlinkMsgId,
            notifyType: 'producerNotification',
            src:  { storeName: newNotify.storeName, address: newNotify.srcStoreIp + ':' + newNotify.srcStorePort, },
            dest: { storeName: newNotify.storeName, serviceName: newNotify.serviceName }
        };

        if (node.manualRead) {
            node.send([null, notifyMessage]);
        } else {
            node.send([null, notifyMessage]);
            readMessage(notifyMessage.redlinkMsgId);
        }
    };

    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId + ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    alasql(createTriggerSql);

    //localStoreConsumers (storeName STRING, serviceName STRING)'); 
    //can have multiple consumers with same name registered to the same store
    const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '")';
    alasql(insertIntoConsumerSql);

    node.on('close', (removed, done) => {
        //todo deregister this consumer
        const dropNotifyTriggerSql = 'DROP TRIGGER ' + msgNotifyTriggerId; //todo this wont work- see https://github.com/agershun/alasql/issues/1113
        //clean up like in the redlinkStore- reinit trigger function to empty
        alasql(dropNotifyTriggerSql);
        log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName + +'"' + 'AND serviceName="' + node.name + '"';
        alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
        log('removed consumer from local store...');
        //TODO use the getlocalNorthSouthConsumers function
        const localConsumersSql  = 'SELECT * FROM localStoreConsumers';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers';
        const localConsumers  = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        done();
    });

    node.on("input", msg => {
        if (msg.cmd === 'read' && node.manualRead) {
            if (msg.redlinkMsgId) {
                readMessage(msg.redlinkMsgId);
            }
        }
    });

    function readMessage(redlinkMsgId) { //todo enforce rate limits here...
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND redlinkMsgId="' + redlinkMsgId + '"';
        const notifies = alasql(notifiesSql);
        if (notifies.length > 0) {
            const sendingStoreName = notifies[0].storeName;
            const address = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
            const options = {
                method: 'POST',
                url:    'https://' + address + '/read-message',
                body:   { redlinkMsgId },
                json:   true
            };
            node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'outBound',Data:options},null]);

            request(options, function (error, response) {
                if (response && response.statusCode === 200) {
                    if (response.body.message) { response.body.message = base64Helper.decode(response.body.message); }
                    const msg = response.body;
                    if(msg){
                        msg.payload = msg.message.payload;
                        msg.topic   = msg.message.topic;
                        delete msg.preserved;
                        delete msg.message;
                        delete msg.read;
                        node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:msg,error:'none'},null]);
                    }
                    node.send(msg);
                } else {  // No message
                    let output = response? response.body: error;
                    if (node.manualRead) {
                        node.send([null, null, output]); //todo rationalise sending outputs- ||| to dlink
                        node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:output,error},null]);
                    } else {
                        node.send([null, output]);
                         node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:output,error},null]);
                    }
            }});
        }
    }
};