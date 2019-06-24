const alasql = require('alasql');

alasql.fn.onchange = function(r) {

    // if(r.a == 123) console.log('Observable event!');

    console.log('got event in trigger callback 1');

};

alasql.fn.onchange1 = function(r) {

    // if(r.a == 123) console.log('Observable event!');

    console.log('got event in trigger callback 2');

};



alasql('CREATE TABLE one (a INT)');

alasql('CREATE TRIGGER two AFTER INSERT ON one CALL onchange()');

alasql('CREATE TRIGGER three BEFORE INSERT ON one CALL onchange1()');

alasql('INSERT INTO one VALUES (123)');  // This will fire onchange()

