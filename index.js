const async = require('async'),
    azure = require('azure-storage'),
    { Connection } = require('topological');

class AzureQueueConnection extends Connection {
    constructor(config) {
        super(config);

        (this.config.visibilityTimeout =
            this.config.visibilityTimeout || 5 * 60), // seconds
            (this.pausedBackoff = this.pausedBackoff || 50); // ms
        this.emptyBackoff = this.pausedBackoff || 1000; // ms
    }

    start(callback) {
        super.start(err => {
            if (err) return callback(err);

            this.azureQueueService = azure.createQueueService(
                this.config.storageAccount,
                this.config.storageKey
            );

            this.azureQueueService.createQueueIfNotExists(
                this.config.queueName,
                callback
            );
        });
    }

    succeeded(message, callback) {
        if (!callback) {
            callback = () => {};
        }
        this.azureQueueService.deleteMessage(
            this.config.queueName,
            message.messageId,
            message.popReceipt,
            callback
        );
    }

    enqueue(messages, callback) {
        async.each(
            messages,
            (message, messageCallback) => {
                let messageText = JSON.stringify(message.body);

                this.log.debug(
                    `enqueing message to queue: ${
                        this.config.queueName
                    }: ${messageText.substr(0, 40)}: ${messageText.length}`
                );

                this.azureQueueService.createMessage(
                    this.config.queueName,
                    messageText,
                    messageCallback
                );
            },
            callback
        );
    }

    dequeueImpl(callback) {
        this.azureQueueService.getMessages(
            this.config.queueName,
            {
                visibilityTimeout: this.config.visibilityTimeout
            },
            (err, messages) => {
                if (err) return callback(err);
                if (messages.length < 1) return callback();

                this.log.debug(
                    `dequeued message from queue: ${
                        this.config.queueName
                    }: ${messages[0].messageText.substr(0, 40)}: ${
                        messages[0].messageText.length
                    }`
                );

                messages[0].body = JSON.parse(messages[0].messageText);

                return callback(null, messages[0]);
            }
        );
    }

    dequeue(callback) {
        let message;

        async.whilst(
            () => {
                return this.started && message === undefined;
            },
            dequeueCallback => {
                if (this.paused) {
                    return setTimeout(dequeueCallback, this.pausedBackoff);
                }

                this.dequeueImpl((err, receivedMessage) => {
                    let backoff = 0;
                    if (err || !receivedMessage) {
                        if (err) this.log.error(`error in dequeue: ${err}`);
                        backoff = this.emptyBackoff;
                    }

                    message = receivedMessage;

                    return setTimeout(dequeueCallback, backoff);
                });
            },
            err => {
                return callback(null, message);
            }
        );
    }

    stream(callback) {
        async.whilst(
            () => {
                return this.started;
            },
            iterationCallback => {
                this.dequeue((err, message) => {
                    callback(err, message);
                    return iterationCallback();
                });
            },
            err => {
                this.log.info(
                    `${
                        this.id
                    }: message loop for input stopping with err: ${err}`
                );
            }
        );
    }
}

module.exports = AzureQueueConnection;
