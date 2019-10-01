const express = require('express');
const https = require('https');
const bodyParser = require('body-parser');
const selfsigned = require('selfsigned');

let app;
const attrs = [{name: 'commonName', value: 'wombat.cassowary.meerkat.com'}];
const pems = selfsigned.generate(attrs, {days: 3650,keySize:2048});

let server;
module.exports.startServer = function (port) {
    app = express();
    app.use(bodyParser.json());
    try {
        server = https.createServer({
            key: pems.private,
            cert: pems.cert
        }, app).listen(port).on( 'error', function (e) {
            if (e.code === 'EADDRINUSE') {
                console.log('Address in use');
            }
        });
        // server.maxConnections = 3;
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