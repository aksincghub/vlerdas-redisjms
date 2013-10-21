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
	logger.trace('Popping Data from ' + config.redis.channel);
	client.blpop(config.redis.channel, '0', callback);
	logger.info('Listening on Redis Channel-' 
		+ (config.redis.host || 'localhost') + ':' 
		+ (config.redis.port || '6379') 
		+ ' \nSource Channel: ' + config.redis.channel 
		+ ' \nJMS Target Destination: ' + config.jms.destinationJndiName);

});
// Do client.auth

function processErrorAndRollback(err, evt) {
	logger.error('Rolling back due to error' + err);
	// Something goes wrong - push event back to queue
	logger.trace('Pushed event back to queue', evt);	
	client.lpush(evt[0], evt[1]);
	// Listen again
	logger.trace('Listen again on the queue', evt);	
	client.blpop(config.redis.channel, '0', callback);
}

function callback(err, evt){
	if (err) {
		if(evt) {
			if (!_.isArray(evt)) {
				processErrorAndRollback(err, evt);
				return;
			}
		} else {
			// retry globally if there is no element
			throw err;
		}
    }

    var channel = evt[0];
    var object = evt[1];


    if (object) {
        logger.trace("Evicted from Queue:" + channel + " Object: " + object);
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
     				processErrorAndRollback(err, evt);
					return;
				}
				logger.info('Sent Message to JMS Queue:' + config.jms.destinationJndiName + ' Message:' + object);
				// Listen again
				logger.info('Listening on Channel:' + config.redis.channel);
				client.blpop(config.redis.channel, '0', callback);
				return;
			});
		});
	}
}
// Default exception handler
process.on('uncaughtException', function (err) {
    logger.error('Caught exception: ' + err);
	// Continue listening
    logger.trace('Continuing to listen on : ' + config.redis.channel);
	client.blpop(config.redis.channel, '0', callback);
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