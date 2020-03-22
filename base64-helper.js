'use strict';

const encodetype = 'utf16le';
const decodetype = 'utf16le';

module.exports.encode = function(obj){
    if(!obj){
        return obj;
    }
    return new Buffer(JSON.stringify(obj), encodetype || 'utf8').toString('base64');

};

module.exports.decode = function(str){//todo change to streams- may have to use https://www.npmjs.com/package/base64-stream
    // or similar- make sure that any library used supports encodingType
    if(!str){
        return str;
    }
    let returnVal = new Buffer(str, 'base64').toString(decodetype || 'utf8');
    try {//todo revisit this- does not work well for large strings- will need to use
        //something like https://www.npmjs.com/package/big-json but then will have to change api of this module
        //instead of passing in string pass in stream- or maybe do this instead- https://stackoverflow.com/a/25650163/2462516
        returnVal = JSON.parse(returnVal);
    } catch (e) {
        console.log(e);
    } finally {
        return returnVal;
    }
};

