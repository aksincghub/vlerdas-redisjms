/**
 * Entry point for the REDIS JMS Bridge Listener.
 *
 * Created by: Julian Jewel
 *
 */
var config = require('config');
var _ = require('underscore');
// Export config, so that it can be used anywhere
module.exports.config = config;
var NodeJms = require('nodejms');
var retry = require('retry');
var Log = require('vcommons').log;
var logger = Log.getLogger('REDIS2JMS', config.log);
var cluster = require("cluster");
var numCPUs = require('os').cpus().length;
var RedisQueue = require('redisqueue');
var uuid = require('node-uuid');
var request = require('request');
var jmsClient = new NodeJms(__dirname + "/" + config.jms.jmsConnectLibrary, config.jms);

// Cluster Setup for LENS
if (cluster.isMaster) {
    // Fork workers.
    for (var i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('online', function (worker) {
        logger.info('A worker with #' + worker.id);
    });
    cluster.on('listening', function (worker, address) {
        logger.info('A worker is now connected to ' + address.address + ':' + address.port);
    });
    cluster.on('exit', function (worker, code, signal) {
        logger.info('worker ' + worker.process.pid + ' died');
    });
} else {

    new RedisQueue(config.redis, function (data, err, callback) {
        if (_.isUndefined(data)) {
            logger.error("Received empty object", data);
            logger.error("Possible elements in processing queue", data);
            return callback(new Error("Empty Object received"));
        }

        var operation = retry.operation(config.jms.retry);
        operation.attempt(function (currentAttempt) {
            if (config.jms.type == 'QUEUE') {
                jmsClient.sendMessageToQueueAsync(data, "text", config.jms.staticHeaders, function (err) {
                    if (operation.retry(err)) {
                        logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
                        return;
                    }
                    if (err) {
                        logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
                        logger.error('Attempting Rollback..');
                        return new Callback(new Error('Retry failed with error:' + error + ' Attempt:' + currentAttempt));
                    }
                    logger.info('Sent Message to JMS Queue:' + config.jms.destinationJndiName + ' Message:' + data);
                    // Listen again
                    logger.info('Listening on Channel:' + config.redis.channel);
                    if (!_.isUndefined(config.audit)) {
                        audit(data);
                    }
                    return callback();
                });
            } else if (config.jms.type == 'TOPIC') {
                jmsClient.sendMessageToTopicAsync(data, "text", config.jms.staticHeaders, function (err) {
                    if (operation.retry(err)) {
                        logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
                        return;
                    }
                    if (err) {
                        logger.error('Retry failed with error:', error, 'Attempt:', currentAttempt);
                        logger.error('Attempting Rollback..');
                        return new Callback(new Error('Retry failed with error:' + error + ' Attempt:' + currentAttempt));
                    }
                    logger.info('Sent Message to JMS Topic:' + config.jms.destinationJndiName + ' Message:' + data);
                    // Listen again
                    logger.info('Listening on Channel:' + config.redis.channel);
                    if (!_.isUndefined(config.audit)) {
                        audit(data);
                    }
                    return callback();
                });
            } else {
                throw new Error("jms.config.type has to be specified as either TOPIC or QUEUE");
            }
        });
    });
}

function audit(data) {
    // Listen again
    logger.info('Auditing:', config.audit.url);
    var auditService = request.post(config.audit.url, function (err, res, body) {});
    var fileName = uuid.v4() + (config.audit.headers.extension ? config.audit.headers.extension : '.dat');
    logger.info('Auditing File:', fileName);
    var form = auditService.form();
    form.append('file', data, {
        contentType : config.audit.headers.contentType,
        filename : fileName
    });
}
// Default exception handler
process.on('uncaughtException', function (err) {
    logger.error('Caught exception: ' + err);
    process.exit()
});
// Ctrl-C Shutdown
process.on('SIGINT', function () {
    logger.info("Shutting down from  SIGINT (Crtl-C)");
    process.exit()
})
// Default exception handler
process.on('exit', function (err) {
    logger.info("Exiting.. Error:", err);
    if (!_.isUndefined(jmsClient)) {
        logger.info("Destroying JMS Connections:", err);
        jmsClient.destroy();
    }
});