Use the settings.js file as is (location depends on your OS- on windows it is C:\Users\<userName>\\.node-red\settings.js)
or use it as a base to tweak existing settings. 
 * redlink specific settings:
 1. largeMessagesDirectory - directory to store large messages. defaults to /tmp/redlink/
 2. largeMessageThreshold - max number of characters transmitted as is before the messaging system persists to disk. The default is 25000 chars approximately. Note that this value depends on a number of factors including the length of the base64 encoded message- hence the stress on the word approximate. 
 
 
