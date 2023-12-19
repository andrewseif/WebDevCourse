//const Kafka = require("kafkajs").Kafka
const {Kafka} = require("kafkajs")
const msg = process.argv[2];
run();
async function run(){
    try
    {
        const kafka = new Kafka({
            clientId: 'myapp',
            brokers: ['127.0.0.1:9094'],
          });

        const producer = kafka.producer();
        console.log("Connecting.....")
        await producer.connect()
        console.log("Connected!")
        //A-M 0 , N-Z 1 
        const partition = msg[0] < "N" ? 0 : 1;
        const result =  await this.client.connect();
        await producer.send({
            "topic": "Users",
            "messages": [
                {
                    "value": 100000,
                    "partition": partition
                }
            ]
        })

        console.log(`Send Successfully! ${JSON.stringify(result)}`)
        await producer.disconnect();
    }
    catch(ex)
    {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }


}