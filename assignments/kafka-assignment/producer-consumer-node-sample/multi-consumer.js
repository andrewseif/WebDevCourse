const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: 'myapp',
  brokers: ['localhost:19092'],
});

// Consumer configuration
const consumerConfig = {
  groupId: 'my-consumer-group',
};

// Create two consumers within the same group
const consumerA = kafka.consumer(consumerConfig);
const consumerB = kafka.consumer(consumerConfig);

run();

async function run() {
  try {
    // Connect both consumers
    await Promise.all([
      consumerA.connect(),
      consumerB.connect(),
    ]);

    console.log("Connected!");

    // Subscribe both consumers to the "Users" topic
    await Promise.all([
      consumerA.subscribe({ topic: "Users", fromBeginning: true }),
      consumerB.subscribe({ topic: "Users", fromBeginning: true }),
    ]);

    // Start consuming messages for consumerA
    await consumerA.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Consumer A - Received message on partition ${partition}: ${message.value}`);
      },
    });

    // Start consuming messages for consumerB
    await consumerB.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(`Consumer B - Received message on partition ${partition}: ${message.value}`);
      },
    });

    // Keep the script running to continue consuming messages
    // You can handle this differently based on your requirements

  } catch (ex) {
    console.error(`Something bad happened ${ex}`);
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  await Promise.all([
    consumerA.disconnect(),
    consumerB.disconnect(),
  ]);
  console.log('Disconnected consumers.');
  process.exit(0);
});

process.on('SIGINT', async () => {
  await Promise.all([
    consumerA.disconnect(),
    consumerB.disconnect(),
  ]);
  console.log('Disconnected consumers.');
  process.exit(0);
});
