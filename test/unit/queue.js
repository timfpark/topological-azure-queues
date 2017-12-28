const assert = require('assert');
const fixtures = require('../fixtures');

describe('AzureQueueConnection', function() {
    it('can queue and dequeue messages', done => {
         fixtures.connection.start(err => {
            assert(!err);
            fixtures.connection.dequeue((err, message) => {
                assert(!err);
                assert(message);

                assert(message.body.number, 1);

                fixtures.connection.complete(message, done);
            });

            fixtures.connection.enqueue([{
                body: {
                    number: 1
                }
            }], err => {
                console.log(err);
                assert(!err);
            });
         });
     });
});
