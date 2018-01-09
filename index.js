const async = require('async'),
      azure = require('azure-storage'),
      { Connection } = require("topological");

class AzureQueueConnection extends Connection {
    constructor(config) {
        super(config);

        this.config.visibilityTimeout = this.config.visibilityTimeout || 5 * 60, // seconds

        this.pausedBackoff = this.pausedBackoff || 50; // ms
        this.emptyBackoff = this.pausedBackoff || 1000; // ms
    }

    start(callback) {
        super.start(err => {
            if (err) return callback(err);

            this.azureQueueService = azure.createQueueService(
                this.config.storageAccount,
                this.config.storageKey
            );

            this.azureQueueService.createQueueIfNotExists(this.config.queueName, callback);
        });
    }

    succeeded(message, callback) {
        if (!callback) {
            callback = () => {};
        }
        this.azureQueueService.deleteMessage(this.config.queueName, message.messageId, message.popReceipt, callback);
    }

    enqueue(messages, callback) {
        async.each(messages, (message, messageCallback) => {
            this.azureQueueService.createMessage(this.config.queueName, JSON.stringify(message.body), messageCallback);
        }, callback);
    }

    dequeueImpl(callback) {
        this.azureQueueService.getMessages(this.config.queueName, {
            visibilityTimeout: this.config.visibilityTimeout
        }, (err, messages) => {
            if (err) return callback(err);
            if (messages.length < 1) return callback();

            messages[0].body = JSON.parse(messages[0].messageText);

            return callback(null, messages[0]);
        });
    }

    dequeue(callback) {
        let message;

        async.whilst(
            () => { return this.started && message === undefined; },
            dequeueCallback => {
                if (this.paused) {
                    return setTimeout(dequeueCallback, this.pausedBackoff);
                }

                this.dequeueImpl((err, receivedMessage) => {
                    let backoff = 0;
                    if (err || !receivedMessage) {
                        if (err) console.error(err);
                        backoff = this.emptyBackoff;
                    }

                    message = receivedMessage;

                    return setTimeout(dequeueCallback, backoff);
                });
            }, err => {
                return callback(null, message);
            }
        );
    }

    stream(callback) {
        async.whilst(
            () => {
                return this.started;
            }, iterationCallback => {
                this.dequeue( (err, message) => {
                    callback(err, message);
                    return iterationCallback();
                });
            }, err => {
                console.log(`${this.id}: message loop for input stopping. err: ${err}`);
            }
        );
    }
}

module.exports = AzureQueueConnection;
