const https = require('https');
const selfsigned = require('selfsigned');
const attrs = [{name: 'commonName', value: 'wombat.abcd.nbnco.com.au'}];
const pems = selfsigned.generate(attrs, {days: 3650});
const express = require('express');
const app = express();
let server;
module.exports.startServer = function (port) {
    try {
        server = https.createServer({
            key: pems.private,
            cert: pems.cert
        }, app).listen(port);
        return server;
    } catch (e) {
        console.log(e); //todo error handling
    }
};

module.exports.getServer = function () {
    return server;
};

module.exports.getExpressApp = function () {
    return app;
};