const amqp = require('amqplib/callback_api');

// Function to simulate the insertion of messages into a database
const insertMany = (messages) => {
      console.log(`Inserting ${messages.length} messages into the database.`);
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log('Messages inserted successfully.');
            resolve();
        }, 500);
    });
};

amqp.connect('amqp://localhost', (err, connection) => {
    if (err) {
        throw err;
    }

    connection.createChannel((err, channel) => {
        if (err) {
            throw err;
        }

        let queueName = 'technical';
        let messageBuffer = [];
        let bufferSize = 1000; // Number of messages to accumulate before processing

        channel.assertQueue(queueName, { durable: false });

        const processMessages = async () => {
            if (messageBuffer.length > 0) {
                const messagesToProcess = messageBuffer.splice(0, bufferSize);
                try {
                    await insertMany(messagesToProcess);
                    // console.log(`Processed ${messagesToProcess.length} messages.`);
                    // Acknowledge messages only after processing
                    //   channel.ackAll(); 
                    // messagesToProcess.forEach(msg => {
                        // channel.ack(msg);
                    //   });
                } catch (error) {
                    console.error('Failed to insert messages:', error);
                }
            }
        };

        channel.consume(queueName, (msg) => {
            if (msg !== null) {
                messageBuffer.push(msg.content.toString());
                // If buffer is full, process immediately
                if (messageBuffer.length >= bufferSize) {
                    processMessages().catch(console.error);
                }
                // Acknowledge message immediately
                channel.ack(msg);
            }
        }, { noAck: false });

        // Process messages every minute
        setInterval(processMessages, 5000);

        console.log(`Waiting for messages in ${queueName}...`);
    });
});
