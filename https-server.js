const express = require('express');
const https = require('https');
const bodyParser = require('body-parser');
const selfsigned = require('selfsigned');

let app;
const attrs = [{name: 'commonName', value: 'wombat.echidna.com'}];
const pems = selfsigned.generate(attrs, {days: 3650,keySize:2048});

let server;
module.exports.startServer = function (port, key , cert) {
    app = express();
    app.use(bodyParser.json({limit: '1024mb'}));
    app.use(bodyParser.urlencoded({limit: '1024mb', extended: true}));
    try {
        const _key = key? key.trim(): pems.private;
        const _cert = key && cert ? cert.trim(): pems.cert;
        server = https.createServer({
            key: _key,
            cert: _cert
        }, app);
        // server.maxConnections = 3;
        return server;
    } catch (e) {
        console.log(e);
        throw e;
    }
};

module.exports.getServer = function () {
    return server;
};

module.exports.getExpressApp = function () {
    return app;
};