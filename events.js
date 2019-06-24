var events = require('events');
var eventEmitter = new events.EventEmitter();

//Create an event handler:
var myEventHandler = function (crap) {
  console.log('I hear a scream!,',crap);
}

//Assign the event handler to an event:
eventEmitter.on('scream', myEventHandler);

//Fire the 'scream' event:
eventEmitter.emit('scream',{crap:'qwqqwqwqwqwqwqwqwq'});
