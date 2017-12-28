const async = require('async'),
      azure = require('azure-storage'),
      { Connection } = require("topological");

class AzureQueueConnection extends Connection {
    constructor(config) {
        super(config);

        this.backoffMilliseconds = this.backoffMilliseconds || 1000;
    }

    start(callback) {
        this.azureQueueService = azure.createQueueService(
            this.config.storageAccount,
            this.config.storageKey
        );

        this.azureQueueService.createQueueIfNotExists(this.config.queueName, callback);
    }

    complete(message, callback) {
        this.azureQueueService.deleteMessage(this.config.queueName, message.messageId, message.popReceipt, callback);
    }

    enqueue(messages, callback) {
        async.each(messages, (message, messageCallback) => {
            this.azureQueueService.createMessage(this.config.queueName, JSON.stringify(message.body), messageCallback);
        }, callback);
    }

    dequeue(callback) {
        this.azureQueueService.getMessages(this.config.queueName, (err, messages) => {
            if (err) return callback(err);

            messages[0].body = JSON.parse(messages[0].messageText);

            return callback(null, messages[0]);
        });
    }
}

module.exports = AzureQueueConnection;
