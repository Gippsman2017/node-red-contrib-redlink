const https = require('/root/github/node-red-contrib-redlink/node_modules/uri-js/dist/esnext/schemes/https.js');
   var sendmessage = ((hostIn,portIn,timeoutIn,pathIn,uuidIn,dataType,dataIn) => {
      var options = {
        hostname           : hostIn,
        port               : portIn,
        path               : pathIn,
        method             : 'POST',
        rejectUnauthorized : false,
        requestCert        : true,
        agent              : false,
        timeout            : timeoutIn,
        headers            : {
         'Content-Type'     : dataType,
         'Content-Length'   : dataIn.length
        } // headers
      }; //options

      return new Promise((resolve, reject) => {
        const req = https.request(options,(res) => {
           var dataOut = '';
           res.on('data', function(d){dataOut += d.toString(); }); // res.on data  
           res.on('error', reject);
           res.on('end',  function () {
              if (res.statusCode >= 200 && res.statusCode <= 299) { resolve(dataOut); } 
              else { reject( 'body: ' + dataOut); }
           }); //res.on end
       }); // res.on
       req.on('error', reject);
       req.write(dataIn, 'binary');
       req.end();
      }); //promise
   }); // sendmessage
   
sendmessage('localhost',8081,5000,'/registration','qwerty','application/json',JSON.stringify({"msg":{ "payload" : 'asdfgh'}}))
    .then ((res) => { console.log(res)})
    .catch((err) => { console.log('BAD' ,err)});
   