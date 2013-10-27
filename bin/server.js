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
var retry= require('retry');
var Log = require('vcommons').log;
var logger = Log.getLogger('REDIS2JMS', config.log);
var JsonFormatter = require('vcommons').jsonFormatter;
var CryptoJS= require('crypto-js');


var jmsClient = new NodeJms(__dirname + "/" + config.jms.jmsConnectLibrary, config.jms); 
logger.info('Setting up Redis Connection to ', config.redis.port, config.redis.host);
var client = redis.createClient(config.redis.port, config.redis.host);

logger.trace('Authenticating Redis with ' + config.redis.auth);
client.auth(config.redis.auth, function (err) {
	if(err) {
		logger.error('Could not authenticate ' +  config.redis.host + ':' + config.redis.port, err);
		throw err;
	}
	logger.info('Authenticated ' +  config.redis.host + ':' + config.redis.port);
	// Start the process
	logger.info('Redis Listening to ' + config.redis.channel);
	logger.trace('Popping Data from ' + config.redis.channel + ' into ' + config.redis.processingChannel + ' with timeout ' + config.redis.timeout);
	client.brpoplpush(config.redis.channel, config.redis.processingChannel, config.redis.timeout, callback);
	logger.info('Listening on Redis Channel-' 
		+ (config.redis.host || 'localhost') + ':' 
		+ (config.redis.port || '6379') 
		+ ' \nSource Channel: ' + config.redis.channel 
		+ ' \nJMS Target Destination: ' + config.jms.destinationJndiName);

});

function callback(err, evt){

    if (err) {
		// Stop execution on error.
		logger.error("Received error", err);
		throw err;
    }
	
	// Process & Continue
	processNotification(err, evt, function(err, evt){
		if(err) {
			logger.error('Error occured, possible elements in processing queue ', evt);
		} else {
			logger.info('Removing from Redis Processing Channel-' + (config.redis.host || 'localhost') + ':' + (config.redis.port || '6379'), config.redis.processingChannel);
			client.lrem(config.redis.processingChannel, 1, evt);
		}
		logger.trace('Popping Data from ' + config.redis.channel + ' into ' + config.redis.processingChannel + ' with timeout ' + config.redis.timeout);
		client.brpoplpush(config.redis.channel, config.redis.processingChannel, config.redis.timeout, callback);
	});
}

function processNotification(err, evt, callback) {
    var object;
    logger.trace("Evicted from Queue:" + evt);
	if(!_.isUndefined(config.redis.encryption) && config.redis.encryption.enabled) {
		logger.trace("Decrypting Event..");
		var decrypted = CryptoJS.AES.decrypt(evt, config.redis.encryption.passPhrase, { format: JsonFormatter });
		logger.trace("Decrypted Event:" + " Event: " + decrypted);
		object = decrypted.toString(CryptoJS.enc.Utf8)
		logger.trace("Decrypted String:" + " Event: " + object);
	}	else {
		object = evt;
	}
	
	if(_.isUndefined(object)) {
		logger.error("Received empty object", object);
		logger.error("Possible elements in processing queue", object);
		return callback(new Error("Empty Object received" + evt), evt);
	}

	var operation = retry.operation(config.jms.retry);
	operation.attempt(function (currentAttempt) {
		jmsClient.sendMessageAsync(object, "text", config.jms.staticHeaders, function (err) {
		   if (operation.retry(err)) {
				logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
				return;
			}
			if(err) {
				logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
				logger.error('Attempting Rollback..');
				return new Callback(new Error('Retry failed with error:' + error + ' Attempt:' + currentAttempt), evt);
			}
			logger.info('Sent Message to JMS Queue:' + config.jms.destinationJndiName + ' Message:' + object);
			// Listen again
			logger.info('Listening on Channel:' + config.redis.channel);
			return callback(null, evt);
		});
	});
}


// Default exception handler
process.on('uncaughtException', function (err) {
    logger.error('Caught exception: ' + err);
});
// Ctrl-C Shutdown
process.on('SIGINT', function () {
    logger.info("Shutting down from  SIGINT (Crtl-C)");
    process.exit()
})
// Default exception handler
process.on('exit', function (err) {
    logger.info("Exiting.. Error:", err);
	if(!_.isUndefined(client)) {
		// Cleanup
	}
	if(!_.isUndefined(jmsClient)) {
		logger.info("Destroying JMS Connections:", err);
		jmsClient.destroy();
	}
});