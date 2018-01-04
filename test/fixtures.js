const AzureQueueConnection = require('../index');

module.exports = {
     connection: new AzureQueueConnection({
          id: "azureQueueConnection",
          config: {
              storageAccount: process.env.LOCATION_STORAGE_ACCOUNT,
              storageKey: process.env.LOCATION_STORAGE_KEY,
              queueName: 'test'
          }
     })
};
