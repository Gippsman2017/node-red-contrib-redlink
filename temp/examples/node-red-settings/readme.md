The settings.js file needs to have a number of entries to cover redlink, however the default settings will normally cover it.

Add the following strings to the settings.js file, note that the defaults as below anyway.

Redlink specific settings:

#Redlink storage 
 1. largeMessagesDirectory - Directory to store large messages. defaults to /tmp/redlink/
 2. largeMessageThreshold  - Is the maximum number of characters transmitted before the messaging system persists message to disk. 

Notes : The default is 25000 chars (approximately), this value depends on the length of the base64 encoded message

#North Peer default address setting is localhost, port 2000

NorthPeers is an array of DNS named or IP Address / Port objects- something like

northPeers:[ 
{address: 'hostname1', port:'2000'},
{address: 'hostname2', port:'2000'} 
...
...
]


 
 
