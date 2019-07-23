const alasql = require('alasql');
const request = require('request').defaults({strictSSL: false});

const base64Helper = require('./base64-helper.js');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkReply = function (config) {
    RED.nodes.createNode(this, config);

    const node = this;
    node.topic = config.topicReply;
   
    node.on("input", msg => {
        if (msg.redlinkMsgId) {
           const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + msg.redlinkMsgId + '"';
           log('$$$$$$$$$$$$$$',msgSql);
           const matchingMessages = alasql(msgSql);
           log('REPLY MATCHING MESSAGE=',matchingMessages);
           node.send([{action:'replySend',direction:'inBound',message:matchingMessages}]);
//           log('in reply matchingMessages:', matchingMessages);
           if (matchingMessages.length > 0) { //should have only one
              const replyStore   = matchingMessages[0].storeName;
              const replyService = matchingMessages[0].serviceName;
              log('Reply notifiy=',  alasql('SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"')); // AND storeName="' + replyStore + '"';
              const notifySql    = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '"'; // AND storeName="' + replyStore + '"';
              const notifies     = alasql(notifySql); //should have only one
              if (notifies.length > 0) {
                 const replyAddress = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
                 delete msg.preserved;
                 const body = {
                      topic : node.topic,
                      replyingService: replyService,
                      redlinkMsgId: msg.redlinkMsgId,
                      payload: base64Helper.encode(msg.payload)
                      };
                        //'INSERT INTO notify VALUES ("' + node.name + '","' + req.body.service + '","' + req.body.srcStoreIp + '",' + req.body.srcStorePort + ',"' + req.body.redlinkMsgId +  '")';
                 const options = {
                      method: 'POST',
                      url: 'https://' + replyAddress + '/reply-message',
                      body,
                      json: true
                      };
                 request(options, function (error, response) {
                    body.payload = base64Helper.decode(body.payload);
                    node.send([{storeName: replyStore,serviceName:replyService,action:'replySend',direction:'outBound',Data:body,error}]);
                  });
               }
           }
        }
    });
};