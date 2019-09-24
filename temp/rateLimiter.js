const RateLimiter = require('limiter').RateLimiter;

module.exports.getRateLimiter = function(numMessages, rate, timeUnit){ //x numMessages in y rate timeUnits-e.g., 5 msgs in 10 seconds
    numMessages = +(numMessages|| 1);
    rate = +(rate || 1);
    timeUnit = timeUnit || 'second';
    let multiplier = 1;
    switch (timeUnit) {
        case 'second':
            multiplier *= 1000;
            break;
        case 'minute':
            multiplier *= 60 * 1000;
            break;
        case 'hour':
            multiplier *= 60 * 60 * 1000;
            break;
        case 'day':
            multiplier *= 24 * 60 * 60 * 1000;
    }
    let msgRate = numMessages / rate;
    if(msgRate < 1){//will be less than 1 when we divide- will cause rate limiter to ignore... so the multiplier needs to be suitably augmented
        multiplier *= 1/msgRate;
        msgRate = 1;
    }
    return new RateLimiter(msgRate, multiplier);
};



/*
    rateTypeSendReceive: 'none', //none or rateLimit
    nbRateUnitsSendReceive: 1, //how many messages   numMessages
    rateSendReceive: 1, //per x time units           rate
    rateUnitsSendReceive: "second" //second/minute/hour/day   timeUnit

 */