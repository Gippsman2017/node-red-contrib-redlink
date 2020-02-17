const base64 = require('base64-coder-node')();

const encodetype = 'utf16le';
const decodetype = 'utf16le';

module.exports.encode = function(obj){
    if(!obj){
        return obj;
    }
    return base64.encode(JSON.stringify(obj), encodetype);
};

module.exports.decode = function(str){ //todo change to streams- may have to use https://www.npmjs.com/package/base64-stream
    // or similar- make sure that any library used supports encodingType
    if(!str){
        return str;
    }
    let returnVal = base64.decode(str, decodetype);
    try {
        returnVal = JSON.parse(returnVal); //todo revisit this- does not work well for large strings- will need to use
        //something like https://www.npmjs.com/package/big-json but then will have to change api of this module
        //instead of passing in string pass in stream- or maybe do this instead- https://stackoverflow.com/a/25650163/2462516
    } catch (e) {
        console.log(e);
    } finally {
        return returnVal;
    }
};