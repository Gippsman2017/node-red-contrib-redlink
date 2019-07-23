const base64 = require('base64-coder-node')();

const encodetype = 'binary';
const decodetype = 'binary';

module.exports.encode = function(obj){
    return base64.encode(JSON.stringify(obj), encodetype);
};

module.exports.decode = function(str){
    return JSON.parse(base64.decode(str, decodetype));
};