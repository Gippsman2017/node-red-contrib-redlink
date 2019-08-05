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
    node.manualRead        = config.manualReadReceiveSend;
    if (node.consumerMeshName) { node.consumerStoreName = node.consumerMeshName + ':' + node.consumerStoreName; } 
                          else { }
                          
    const msgNotifyTriggerId  = 'a' + config.id.replace('.', '');
    const newMsgNotifyTrigger = 'onNotify' + msgNotifyTriggerId;



    alasql.fn[newMsgNotifyTrigger] = () => {
        //check if the notify is for this consumer name with the registered store name

        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' +
                                                                       node.name + '"' + ' AND notifySent NOT LIKE "%' + node.id + '%"';
        const notifies = alasql(notifiesSql);
        const newNotify = notifies[notifies.length - 1];
        

        if (!newNotify) { return; } //nothing to do- trigger for some other service

        const notifiesSql3 = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND serviceName="' +
                                                                       node.name + '"' ;
        const notifies3 = alasql(notifiesSql3); //todo- remove this?

        // OK, this consumer will now add its own node.id to the notify trigger message since it comes in without one.
        const existingNotifiedNodes = newNotify.notifySent.trim();
        let newNotifiedNodes = existingNotifiedNodes ? existingNotifiedNodes + ',' + node.id : node.id;
        const updateNotifySql = 'UPDATE notify SET notifySent="' + newNotifiedNodes + '" WHERE redlinkMsgId="' + newNotify.redlinkMsgId + '" AND storeName="' + node.consumerStoreName + '"';
        alasql(updateNotifySql);

        const notifyMessage = {
            redlinkMsgId: newNotify.redlinkMsgId,
            notifyType: 'producerNotification',
            src:  { storeName: newNotify.storeName, address: newNotify.srcStoreIp + ':' + newNotify.srcStorePort, },
            dest: { storeName: newNotify.storeName, serviceName: newNotify.serviceName, consumer: node.id }
        };

        if (node.manualRead) {
           sendMessage({ notify: notifyMessage });
//             sendMessage(msgnode.send([null, notifyMessage]);
        } else {
            sendMessage({ notify: notifyMessage }); //send notify regardless of whether it is manual or auto read
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
//        log('dropped notify trigger...');
        const deleteConsumerSql = 'DELETE FROM localStoreConsumers WHERE storeName="' + node.consumerStoreName + +'"' + 'AND serviceName="' + node.name + '"';
        alasql(deleteConsumerSql); //can have multiple consumers with same name registered to the same store
//        log('removed consumer from local store...');
        //TODO use the getlocalNorthSouthConsumers function
        const localConsumersSql  = 'SELECT * FROM localStoreConsumers';
        const globalConsumersSql = 'SELECT * FROM globalStoreConsumers';
        const localConsumers  = alasql(localConsumersSql);
        const globalConsumers = alasql(globalConsumersSql);
        done();
    });




    node.on("input", msg => {
        if (msg.cmd === 'read') {
          if (node.manualRead) {
               let mycb='0'; //todo remvoe this?

            if (msg.redlinkMsgId) { 
               readMessage(notifies[0].redlinkMsgId); //todo ask John- why are we reding the first notify instead of msg.redlinkMsgId?
              }
         else
             {// should be here for a normal read
                 //TODO ask John- auto read should happen automatically from the triggers- why is this needed?
               const notifiesSql = 'SELECT redlinkMsgId from notify WHERE storeName="' + node.consumerStoreName + '"  and notifySent = "'+node.id+'"';
               const notifies    = alasql(notifiesSql);
                 console.log('Notifies = ',notifies);
               const numberOfNotifies = notifies.length;
               if (numberOfNotifies > 0) {
                    readMessage(notifies[0].redlinkMsgId);
               }    
             else
               {
                 //  sendMessage({ failure: {"error":"Store1 " + node.consumerStoreName + " Does NOT have any notifies for this service " + node.name + " consumer "+node.id}});
               }
            }
        } //manual read
      }  //cmd read 
   else

     {  // Reply message, this is where the reply is actually sent back to the replyMessages on the Producer.
     
         if (msg.redlinkMsgId && !msg.sendOnly) {

            const notifiesSql = 'SELECT redlinkMsgId from notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '" and storeName="' + node.consumerStoreName + '"  and notifySent = "'+node.id+'"';
            const notifies    = alasql(notifiesSql);

            const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + msg.redlinkMsgId + '"';
            const matchingMessages = alasql(msgSql);
            sendMessage({debug: {action:'replySend',direction:'inBound',message:matchingMessages}});
            // node.send([]);
            if (matchingMessages.length > 0) { //should have only one
              const replyStore        = matchingMessages[0].storeName;
              const replyService      = matchingMessages[0].serviceName;
              const redlinkProducerId = matchingMessages[0].redlinkProducerId;
              const notifySql         = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"and notifySent="'+node.id+'"';
              const notifies          = alasql(notifySql); //should have only one

              if (notifies.length > 0) {
                 const replyAddress = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
                 //                 delete msg.preserved;
                 const body = {
                      replyingService:   replyService,
                      redlinkMsgId:      msg.redlinkMsgId,
                      redlinkProducerId: redlinkProducerId,
                      payload:           base64Helper.encode(msg.payload)
                      };
                 const options = {
                      method: 'POST',
                      url:    'https://' + replyAddress + '/reply-message',
                      body,
                      json:    true
                      };
                 request(options, function (error, response) {
                    body.payload = base64Helper.decode(body.payload);
                  });
               }
            }   
          // OK, I have completed the whole job and sent the reply, now to finally remove the original Notifiy for thi job.
          const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' +  msg.redlinkMsgId + '" and storeName = "'+node.consumerStoreName+ '" and notifySent = "'+node.id+'"';
          const deleteNotify    = alasql(deleteNotifyMsg);
        }
     }        

    });
  

    function sendMessage(msg) { //receive, notify, failure, debug
        const msgs = [];
        if (msg.receive) { msgs.push(msg.receive); } 
                    else { msgs.push(null); }
        if (msg.notify)  { msgs.push(msg.notify); } 
                    else { msgs.push(null); }
        if (msg.failure) { msgs.push(msg.failure); } 
                    else { msgs.push(null); }
        if (node.debug)  {
            if (msg.debug)  {  msgs.push(msg.debug); } 
                       else {  msgs.push(null); }
        }
        node.send(msgs);
    }

    function readMessage(redlinkMsgId) { //todo enforce rate limits here...
        //todo make readMessage just return the message- dont send to outputs- will need to promisify as we are doing a http call
        const notifiesSql = 'SELECT * from notify WHERE storeName="' + node.consumerStoreName + '" AND redlinkMsgId="' + redlinkMsgId + '" and notifySent = "'+node.id+'"';
        const notifies    = alasql(notifiesSql);
        if (notifies.length > 0) {
            const sendingStoreName = notifies[0].storeName;
            const address = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
            const options = {
                method: 'POST',
                url:    'https://' + address + '/read-message',
                body:   { redlinkMsgId },
                json:   true
            };
            sendMessage({ debug: {"debugData": "storeName " + sendingStoreName + ' ' + node.name + "action:consumerRead" + options}});
            //            node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'outBound',Data:options},null]);
            request(options, function (error, response) {
                if (response && response.statusCode === 200) {
                    if (response.body.message) { response.body.message = base64Helper.decode(response.body.message); }
                    const msg = response.body;
                    if(msg){
                        msg.payload = msg.message.payload;
                        delete msg.preserved;
                        delete msg.message;
                        delete msg.read;
                        const receiveMsg = {storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',msg:msg,redlinkMsgId:redlinkMsgId,error:false};
                        // node.send([null,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',msg:msg,redlinkMsgId:redlinkMsgId,error:false},null]);
                        //receive, notify, failure, debug
                        sendMessage({receive: receiveMsg})
                    }
                    sendMessage({failure: {error:'Empty response got when reading message'}})
                    // node.send(msg);
                } 
              else 
                if (response && response.statusCode === 404) {
                    if (response.body.message) { response.body.message = base64Helper.decode(response.body.message); }
                    const msg = response.body;
                    if(msg){
                       // OK the store has told me the message is no longer available, so I will remove this notify
                       const errorMessage = {storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',msg:msg,redlinkMsgId:redlinkMsgId,error:true};
                       sendMessage({failure: errorMessage});
                       const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' +  redlinkMsgId + '" and storeName = "'+node.consumerStoreName+ '" and notifySent = "'+node.id+'"';
                       const deleteNotify    = alasql(deleteNotifyMsg);
//                    console.log('DELETEING Already Read NOTIFY (READMSG)=',deleteNotifyMsg,' = ',deleteNotify);                   
                    }
                } 
              else 
                {  // No message
                    let output = response? response.body: error;
                    if (true/*node.manualRead*/) { //todo- discuss with John and remove else block
                        const errorMessge = {storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',msg:output.msg,redlinkMsgId:output.redlinkMsgId,error:output.error};
                        sendMessage({failure: errorMessge})
                        // node.send([output,,null]);
                    } 
/*
                  else
                    {
                         node.send([output,{storeName: sendingStoreName,consumerName:node.name,action:'consumerRead',direction:'inBound',msg:output.msg,redlinkMsgId:output.redlinkMsgId,error:output.error},null]);
                    }
*/
                   // OK the store has told me the message is no longer available, so I will remove this notify
                   const deleteNotifyMsg = 'DELETE from notify WHERE redlinkMsgId = "' +  redlinkMsgId + '" and storeName = "'+node.consumerStoreName+ '" and notifySent = "'+node.id+'"';
                   const deleteNotify    = alasql(deleteNotifyMsg);
//                   sendMessage({ failure: {"error":"Store " + node.consumerStoreName + " Does NOT have any notifies for this service " + node.name + " consumer "+node.id}});
                }
            }); //request
        } //notifies
    } //readMessage
};