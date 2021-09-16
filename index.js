require('dotenv').config();

const kafka = require('./kafka')
const { v4: uuidv4 } = require('uuid');

const groupId = process.env.GROUP_ID ? process.env.GROUP_ID : uuidv4();
console.log(`Consumer group: ${groupId}`)

const consumer = kafka.consumer({groupId})

const main = async () => {
    await consumer.connect()

    process.env.TOPICS.split(/\s+/).forEach(async topic => {
        await consumer.subscribe({
            topic,
            fromBeginning: true
        })
    })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log('Received message', {
                topic,
                partition,
                offset: message.offset,
                headers: message.headers,
                key: message.key?.toString(),
                value: message.value.toString()
            })
        }
    })
}

main().catch(async error => {
    console.error(error)
    try {
        await consumer.disconnect()
    } catch (e) {
        console.error('Failed to gracefully disconnect consumer', e)
    }
    process.exit(1)
})
