const async = require('async'),
      azure = require('azure-storage'),
      { Connection } = require("topological");

class AzureQueueConnection extends Connection {
    constructor(config) {
        super(config);

        this.config.queueBackoff = 1000;
    }

    start(callback) {
        this.azureQueueService = azure.createQueueService(
            this.config.storageAccount,
            this.config.storageKey
        );

        this.azureQueueService.createQueueIfNotExists(this.config.queueName, callback);
    }

    complete(message, callback) {
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
        this.azureQueueService.getMessages(this.config.queueName, (err, messages) => {
            if (err) return callback(err);
            if (messages.length < 1) return callback();

            messages[0].body = JSON.parse(messages[0].messageText);

            return callback(null, messages[0]);
        });
    }

    dequeue(callback) {
        let message;

        async.whilst(
            () => { return message === undefined; },
            dequeueCallback => {
                this.dequeueImpl((err, receivedMessage) => {
                    let backoff = 0;
                    if (err || !receivedMessage) {
                        if (err) console.error(err);
                        //console.log(`${this.id}: azure queue backing off: ${this.config.queueBackoff}`);
                        backoff = this.config.queueBackoff;
                    }

                    message = receivedMessage;

                    return setTimeout(dequeueCallback, backoff);
                });
            }, err => {
                return callback(null, message);
            }
        );
    }
}

module.exports = AzureQueueConnection;
