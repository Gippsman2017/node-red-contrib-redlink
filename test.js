var   alasql     = require('alasql');


   function doSQL(msg) {
//console.log('doSQL=',msg);
     var   sql       = msg.sql || 'SELECT * FROM ?';
     var   bind      = Array.isArray(msg.payload) ? msg.payload : [msg.payload];
     return alasql.promise(sql, [bind])
       .then (function (res) {
     console.log('RESULT=',res);
          msg.sqlResult = res;
          return msg;
         })
       .catch((err) => {
          msg.error = err;
          return msg;
         });
       };

//var test1 = alasql.compile('create table a (qwerty)');
//var test2 = alasql.compile('select * from a');
    var q=doSQL('create table one (a,b)');
    var q=doSQL('select * from one');
    var ins = alasql.compile('INSERT INTO one VALUES (?,?)'); // no DBname needed
    q();
    ins(1,10);
    ins(2,20);
//console.log('test=',test1(),test2());
