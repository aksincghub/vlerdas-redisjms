/**
 * Entry point for the REDIS JMS Bridge Listener. 
 *
 * Created by: Julian Jewel
 *
 */
var config = require('config');
var redis = require('redis');
var _ = require('underscore');
// Export config, so that it can be used anywhere
module.exports.config = config;
var NodeJms= require('nodejms');

var jmsClient = new NodeJms(__dirname + "/" + config.destination.jmsConnectLibrary, config.destination); 
var client = redis.createClient(config.listener.redis.port || '6379', config.listener.redis.host || 'localhost');
// Do client.auth

function processErrorAndRollback(err, evt) {
	if(config.debug) {
		console.log(err);
	}
	// Something goes wrong - push event back to queue
	client.lpush(evt[0], evt[1]);
	// Listen again
	client.blpop(config.listener.channel, '0', callback);
}

function callback(err, evt){
	if(!_.isArray(evt)) {
		throw new Error("Event must have some value");
	}
	var channel = evt[0];
	var object = evt[1];
	
	if(err) {
		processErrorAndRollback(err, evt);
		return;
	}	
	if(object) {
		
		if(config.debug) {
			console.log("Evicted from Queue:" + channel + " Object: " + object);
		}
		
		jmsClient.sendMessageAsync(object, "text", config.destination.staticHeaders, function (err) {
			if(err) {
				processErrorAndRollback(err, evt);
				return;
			}
			if(config.debug) {
				console.log('Sent Message to JMS Queue:' + config.destination.destinationJndiName + ' Message:' + object);
			}
			// Listen again
			client.blpop(config.listener.channel, '0', callback);
			return;
		});
	}
}
// Start the process
client.blpop(config.listener.channel, '0', callback);

if(config.debug) {
	console.log('Listening on Redis Channel-' 
		+ (config.listener.redis.host || 'localhost') + ':' 
		+ (config.listener.redis.port || '6379') 
		+ ' \nSource Channel: ' + config.listener.channel 
		+ ' \nJMS Target Destination: ' + config.destination.destinationJndiName);
}

// Default exception handler
process.on('uncaughtException', function (err) {
    console.log('Caught exception: ' + err);
});
// Ctrl-C Shutdown
process.on( 'SIGINT', function() {
  console.log( "\nShutting down from  SIGINT (Crtl-C)" )
  process.exit( )
})
// Default exception handler
process.on('exit', function (err) {
	if(!_.isUndefined(client)) {
		// Cleanup
	}
	if(!_.isUndefined(jmsClient)) {
		jmsClient.destroy();
	}
});