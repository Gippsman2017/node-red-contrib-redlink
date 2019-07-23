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
    node.consumerMeshName  = config.consumerMeshName;
    node.manualRead = config.manualReadReceiveSend;
    if (node.consumerMeshName) {
        node.consumerStoreName = node.consumerMeshName + ':' + node.consumerStoreName;
    } else {
        log('\n\n\n\nNo mesh name set for consumer', node.name);
    }
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;
    log('in constructor of consumer:', node.name);

    alasql.fn[newMsgNotifyTrigger] = () => {
        //check if the notify is for this consumer name with the registered store name
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' + node.name + '"' + ' AND notifySent NOT LIKE "%' + node.id + '%"';
        log('in consumer notify trigger all notifies:', alasql('SELECT * from notify'));
        log('notifiesSql for consumer:', node.id, notifiesSql);
        const notifies = alasql(notifiesSql);
        log('notifies for consumer:', node.id, notifies);
        const newNotify = notifies[notifies.length - 1];
        if (!newNotify) {
            return; //nothing to do- trigger for some other service
        }
        const existingNotifiedNodes = newNotify.notifySent.trim();
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const matchingDogs = alasql('SELECT * FROM notify WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND serviceName="' + node.name + '"');
        const updateNotifySql = 'UPDATE notify SET notifySent="' + newNotifiedNodes + '" WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND storeName="' + node.consumerStoreName + '"';
        log('\n\n\n\n!@#$%$#@!\n going to update the following docs:', matchingDogs, ' with updateSql:', updateNotifySql);
        // log('\n\n\n\ngoing to update notifies with', updateNotifySql);
        alasql(updateNotifySql);
        log('!@#$%$#@! after updating all notifies:', alasql('SELECT * FROM notify'));
        const notifyMessage = {
            redlinkMsgId: newNotify.redlinkMsgId,
            notifyType: 'producerNotification',
            src:  { storeName: newNotify.storeName, address: newNotify.srcStoreIp + ':' + newNotify.srcStorePort, },
            dest: { storeName: newNotify.storeName, serviceName: newNotify.serviceName }
        };

        if (node.manualRead) { node.send([null, notifyMessage,notifyMessage]); } 
     else { 
            node.send([null, notifyMessage,notifyMessage]);    readMessage(notifyMessage.redlinkMsgId);
          }
    };

    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId + ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    log('the sql statement for adding trigger in consumer is:', createTriggerSql);
    alasql(createTriggerSql);
    log('registered notify trigger (', createTriggerSql, ') for service ', node.name, ' in store ', node.consumerStoreName);

    //localStoreConsumers (storeName STRING, serviceName STRING)'); 
    //can have multiple consumers with same name registered to the same store
    const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + node.consumerStoreName + '","' + node.name + '")';
    log('in consumer constructor sql to insert into localStoreConsumer is:', insertIntoConsumerSql);
    alasql(insertIntoConsumerSql);
    log('inserted consumer ', node.name, ' for store ', node.consumerStoreName);

    node.on('close', (removed, done) => {
        //todo deregister this consumer
        log('in close of consumer...', node.name);
        const dropNotifyTriggerSql = 'DROP TRIGGER ' + msgNotifyTriggerId; //todo this wont work- see https://github.com/agershun/alasql/issues/1113
        //clean up like in the redlinkStore- reinit trigger function to empty
        alasql(dropNotifyTriggerSql);
        log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName + +'"' + 'AND serviceName="' + node.name + '"';
        alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
        log('removed consumer from local store...');
        //TODO use the getlocalNorthSouthConsumers function
        const localConsumersSql = 'SELECT * FROM localStoreConsumers';
        const localConsumers = alasql(localConsumersSql);
        log('all local consumers are:', localConsumers);
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers';
        const globalConsumers = alasql(globalConsumersSql);
        log(' Global consumers are:', globalConsumers);
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
    console.log('READMESSAGE =', node.consumerStoreName );
        var  notifiesSql = 'SELECT * from notify WHERE storeName="'+node.consumerStoreName+'" AND redlinkMsgId="' + redlinkMsgId + '"';
        var  notifies = alasql(notifiesSql);
        console.log('%%%%%%%%%%%%%%%%%%%%%%%%%Consumer (',node.name,') notifiesSql in consumer:', notifies);
//        if (true) {
        if (notifies.length > 0) {
            const sendingStoreName = notifies[0].storeName;
            const address = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
            const options = {
                method: 'POST',
                url: 'https://' + address + '/read-message',
                body: {
                    redlinkMsgId
                },
                json: true
            };
            node.send([null,null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'outBound',Data:options}]);

            request(options, function (error, response) {
                log(response ? response.statusCode : error);
                if (response && response.statusCode === 200) {
                    if (response.body.message) { response.body.message = base64Helper.decode(response.body.message); }
                    const msg = response.body;
                    if(msg){
                        msg.payload = msg.message.payload;
                        msg.topic   = msg.message.topic;
                        delete msg.preserved;
                        delete msg.message;
                        delete msg.read;
                        log('RESPONSE=', response.body);
                        node.send([null,null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:msg,error:'none'}]);
        
                    log('Response Consumer sendingStore (',node.name,')=',sendingStoreName);
                    }
                    node.send(msg,null,null);
                } else {  // No message
                    let output = response? response.body: error;
                    if (node.manualRead) { node.send([null,null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:output,error}]);} 
                                    else { node.send([null,null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',Data:output,error}]);}
                    
                    log('delete notifiesSql in consumer:', notifiesSql);
                    notifies = alasql(notifiesSql);
                }});
            }
        }
    };