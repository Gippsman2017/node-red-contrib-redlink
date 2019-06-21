const alasql = require('alasql');

let RED;
module.exports.initRED = function (_RED) {
    RED = _RED;
};

module.exports.RedLinkReply = function (config) {
    RED.nodes.createNode(this, config);
    console.log('reply config:', JSON.stringify(config, null, 2));
    var node = this;
};