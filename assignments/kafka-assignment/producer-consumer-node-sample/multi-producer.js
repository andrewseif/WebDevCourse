const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: 'myapp',
  brokers: ['localhost:19092'],
});

const producer = kafka.producer();

run();

async function run() {
  try {
    // Connect the producer
    await producer.connect();
    console.log("Producer Connected!");

    // Send messages to the "Users" topic
    for (let i = 0; i < 10; i++) {
      const message = `Message ${i}`;
      const partition = i % 3; // Use a simple logic to decide partition

      const result = await producer.send({
        topic: "Users",
        messages: [
          {
            value: message,
            partition: partition,
          },
        ],
      });

      console.log(`Producer - Sent message ${i}: ${JSON.stringify(result)}`);
    }

  } catch (ex) {
    console.error(`Something bad happened ${ex}`);
  } finally {
    // Disconnect the producer when done
    await producer.disconnect();
    console.log('Producer Disconnected.');
    process.exit(0);
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  await producer.disconnect();
  console.log('Producer Disconnected.');
  process.exit(0);
});

process.on('SIGINT', async () => {
  await producer.disconnect();
  console.log('Producer Disconnected.');
  process.exit(0);
});
