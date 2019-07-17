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

    node.on("input", msg => {
        if (msg.redlinkMsgId) {
            if (!msg.sendOnly) {
                const msgSql = 'SELECT * FROM inMessages WHERE redlinkMsgId="' + msg.redlinkMsgId + '"';
                const matchingMessages = alasql(msgSql);
                console.log('in reply matchingMessages:', matchingMessages);
                if (matchingMessages.length > 0) { //should have only one
                    const replyStore = matchingMessages[0].storeName;
                    const replyService = matchingMessages[0].serviceName;
                    const notifySql = 'SELECT * FROM notify WHERE redlinkMsgId="' + msg.redlinkMsgId + '" AND storeName="' + replyStore + '"';
                    const notifies = alasql(notifySql); //should have only one
                    console.log('in reply matching notifies:', notifies);
                    if (notifies.length > 0) {
                        const replyAddress = notifies[0].srcStoreIp + ':' + notifies[0].srcStorePort;
                        const body = {
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
                        console.log('going to post to reply-message with options:', JSON.stringify(options, null, 2));
                        request(options, function (error, response) {
                            console.log('in the reply block got response from remote store as:', response ? response.body : error);
                            //todo send response/error to appropriate reply outputs
                        });
                    }
                }
            }
        }
    });
};