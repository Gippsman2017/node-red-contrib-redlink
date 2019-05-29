module.exports = function response (msg, ip) {
  
    let domain = msg.slice(12, msg.length - 4);
    let offset = domain.length + 14;
    let buf = new Buffer(offset);
    domain.copy(buf, 0);
    offset = domain.length;
    buf.writeUInt16BE(1,  offset);
    buf.writeUInt16BE(1,  offset + 2);
    buf.writeUInt32BE(10, offset + 4);
    buf.writeUInt16BE(4,  offset + 8);
    buf.writeUInt32BE(numify(ip), offset + 10);
//    console.log('numify=',numify(ip));  
    return buf;
}

function numify(ip) {
    ip = ip.split('.').map(function(n) {
        return parseInt(n, 10);
    });
    let result = 0;
    let base   = 1;

    for (let i = ip.length-1; i >= 0; i--) {
        result += ip[i]*base;
        base *= 256;
    }
    return result;
}

