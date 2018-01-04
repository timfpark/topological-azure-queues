const assert = require('assert');
const fixtures = require('../fixtures');

describe('AzureQueueConnection', function() {
    it('can enqueue, pause, resume, and stream messages', done => {
        let receivedMessage = false;

        fixtures.connection.start(err => {
            assert(!err);
            fixtures.connection.stream((err, message) => {
                assert(!err);
                assert(message);

                assert(message.body.number, 1);

                fixtures.connection.complete(message, done);
            });

            // wait so that we test retries
            setTimeout( () => {
                fixtures.connection.pause(err => {
                    assert(!err);
                    fixtures.connection.enqueue([{
                        body: {
                            number: 1
                        }
                    }], err => {
                        assert(!err);
                        fixtures.connection.resume(err => {
                            assert(!err);
                        });
                    });
                });
            }, 2000);
        });
     });
});
