const https = require('https');
const selfsigned = require('selfsigned');
const attrs = [{name: 'commonName', value: 'wombat.abcd.nbnco.com.au'}];
const pems = selfsigned.generate(attrs, {days: 3650});
let server;
module.exports.startServer = function (port) {
    try {
        server = https.createServer({
            key: pems.private,
            cert: pems.cert
        }, (req, res) => { //server to listen on inbound messages- notify from producers; register from consumers/downstream branches
            //todo handle notify
            //todo handle register
            res.writeHead(200);
            res.end("hello world\n");
            console.log(req.url);
        }).listen(port);
    } catch (e) {
        console.log(e); //todo robust error handling
    }
};

module.exports.getServer = function () {
    return server;
};