const {Kafka} = require("kafkajs")

run();
async function run(){
    try
    {
        const kafka = new Kafka({
            clientId: 'myapp',
            brokers: ['127.0.0.1:9094']
          });

        const admin = kafka.admin();
        console.log("Connecting.....")
        await admin.connect()
        console.log("Connected!")
        //A-M, N-Z
        await admin.createTopics({
            "topics": [{
                "topic" : "sabry-gamed",
                "numPartitions": 3
            }]
        })
        console.log("Created Successfully!")
        await admin.disconnect();
    }
    catch(ex)
    {
        console.error(`Something bad happened ${ex}`)
    }
    finally{
        process.exit(0);
    }


}