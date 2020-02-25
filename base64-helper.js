const base64 = require('base64-coder-node')();

const encodetype = 'utf16le';
const decodetype = 'utf16le';

module.exports.encode = function(obj){
    if(!obj){
        return obj;
    }
    return base64.encode(JSON.stringify(obj), encodetype);
};

module.exports.decode = function(str){
    if(!str){
        return str;
    }
    let returnVal = base64.decode(str, decodetype);
    try {
        returnVal = JSON.parse(returnVal);
    } catch (e) {
        console.log(e);
    } finally {
        return returnVal;
    }
};