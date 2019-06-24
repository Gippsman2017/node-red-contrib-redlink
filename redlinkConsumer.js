const alasql = require('alasql');
let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkConsumer = function (config) {
    RED.nodes.createNode(this, config);
    this.name = config.name;
    this.consumerStoreName = config.consumerStoreName;
    const msgNotifyTriggerId = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;
    console.log('in constructor of consumer:', this.name);
    alasql.fn[newMsgNotifyTrigger] = () => {
        //check if the notify is for this consumer name with the registered store name
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + this.consumerStoreName + '" AND serviceName="' + this.name + '"';
        console.log('notifiesSql in consumer:', notifiesSql);
        var notifies = alasql(notifiesSql);
        console.log('notifies for this consumer:', notifies);
        this.send([notifies[0], null]);
    };
    const createTriggerSql = 'CREATE TRIGGER ' + msgNotifyTriggerId +
        ' AFTER INSERT ON notify CALL ' + newMsgNotifyTrigger + '()';
    console.log('the sql statement for adding trigger in consumer is:', createTriggerSql);
    alasql(createTriggerSql);
    console.log('registered notify trigger (',createTriggerSql,') for service ', this.name, ' in store ', this.consumerStoreName);

    //localStoreConsumers (storeName STRING, serviceName STRING)'); 
    //can have multiple consumers with same name registered to the same store
    const insertIntoConsumerSql = 'INSERT INTO localStoreConsumers ("' + this.consumerStoreName + '","' + this.name + '")'; 
    console.log('in consumer constructor sql to insert into localStoreConsumer is:', insertIntoConsumerSql);
    alasql(insertIntoConsumerSql);
    console.log('inserted consumer ', this.name, ' for store ', this.consumerStoreName);

    this.on('close', (removed, done) => {
        //todo deregister this consumer
        console.log('in close of consumer...', this.name);
        const dropNotifyTriggerSql = 'DROP TRIGGER ' + msgNotifyTriggerId;
        alasql(dropNotifyTriggerSql);
        console.log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + this.consumerStoreName + +'"' + 'AND serviceName="' + this.name + '"';
        alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
        console.log('removed consumer from local store...');
        //TODO use the getlocalNorthSouthConsumers function
        const localConsumersSql = 'SELECT * FROM localStoreConsumers';
        const localConsumers = alasql(localConsumersSql);
        console.log('all local consumers are:', localConsumers);
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers';
        const globalConsumers = alasql(globalConsumersSql);
        console.log(' Global consumers are:', globalConsumers);
        console.log();
        done();
    });

};