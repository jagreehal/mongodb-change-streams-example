import { MongoClient } from 'mongodb';
import stream from 'stream';

async function main() {
  const uri = process.env.MONGO_DB_URI || 'mongodb://localhost:27017';

  const client: MongoClient = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected successfully to mongodb');

    const pipeline: any = [
      {
        $match: {
          operationType: 'insert',
          'fullDocument.address.country': 'Australia',
          'fullDocument.address.market': 'Sydney',
        },
      },
    ];

    // This script contains three ways to monitor new listings in the listingsAndReviews collection.
    // Comment in the monitoring function you'd like to use.

    // OPTION ONE: Monitor new listings using EventEmitter's on() function.
    await monitorListingsUsingEventEmitter(client, 30000);
    // await monitorListingsUsingEventEmitter(client, 30000, pipeline);

    // OPTION TWO: Monitor new listings using ChangeStream's hasNext() function
    // await monitorListingsUsingHasNext(client, 30000, pipeline);

    // OPTION THREE: Monitor new listings using the Stream API
    // await monitorListingsUsingStreamAPI(client, 30000, pipeline);
  } finally {
    // Close the connection to the MongoDB cluster
    await client.close();
  }
}

main().catch(console.error);

function closeChangeStream(timeInMs = 60000, changeStream): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(() => {
      console.log('Closing the change stream');
      changeStream.close();
      resolve();
    }, timeInMs);
  });
}

async function monitorListingsUsingEventEmitter(
  client: MongoClient,
  timeInMs = 60000,
  pipeline: Document[] = []
) {
  const collection = client
    .db('sample_airbnb')
    .collection('listingsAndReviews');

  const changeStream = collection.watch(pipeline);

  changeStream.on('change', (next) => {
    console.log(next);
  });

  await closeChangeStream(timeInMs, changeStream);
}

/**
 * Monitor listings in the listingsAndReviews collections for changes
 * This function uses the hasNext() function from the MongoDB Node.js Driver's ChangeStream class to monitor changes
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {Number} timeInMs The amount of time in ms to monitor listings
 * @param {Object} pipeline An aggregation pipeline that determines which change events should be output to the console
 */
async function monitorListingsUsingHasNext(
  client,
  timeInMs = 60000,
  pipeline = []
) {
  const collection = client
    .db('sample_airbnb')
    .collection('listingsAndReviews');

  // See https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#watch for the watch() docs
  const changeStream = collection.watch(pipeline);

  // Set a timer that will close the change stream after the given amount of time
  // Function execution will continue because we are not using "await" here
  closeChangeStream(timeInMs, changeStream);

  // We can use ChangeStream's hasNext() function to wait for a new change in the change stream.
  // See https://mongodb.github.io/node-mongodb-native/3.6/api/ChangeStream.html for the ChangeStream docs.
  try {
    while (await changeStream.hasNext()) {
      console.log(await changeStream.next());
    }
  } catch (error) {
    if (changeStream.isClosed()) {
      console.log(
        'The change stream is closed. Will not wait on any more changes.'
      );
    } else {
      throw error;
    }
  }
}

/**
 * Monitor listings in the listingsAndReviews collection for changes
 * This function uses the Stream API (https://nodejs.org/api/stream.html) to monitor changes
 * @param {MongoClient} client A MongoClient that is connected to a cluster with the sample_airbnb database
 * @param {Number} timeInMs The amount of time in ms to monitor listings
 * @param {Object} pipeline An aggregation pipeline that determines which change events should be output to the console
 */
async function monitorListingsUsingStreamAPI(
  client,
  timeInMs = 60000,
  pipeline = []
) {
  const collection = client
    .db('sample_airbnb')
    .collection('listingsAndReviews');

  // See https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#watch for the watch() docs
  const changeStream = collection.watch(pipeline);

  // See https://mongodb.github.io/node-mongodb-native/3.6/api/ChangeStream.html#stream for the stream() docs
  changeStream.stream().pipe(
    new stream.Writable({
      objectMode: true,
      write: function (doc, _, cb) {
        console.log(doc);
        cb();
      },
    })
  );

  // Wait the given amount of time and then close the change stream
  await closeChangeStream(timeInMs, changeStream);
}
