const amqp = require('amqplib/callback_api');

amqp.connect('amqp://localhost', (err, connection) => {
  if (err) {
    throw err;
  }

  connection.createChannel((err, channel) => {
    if (err) {
      throw err;
    }

    let queueName = "technical";

    channel.assertQueue(queueName, {
      durable: false
    });

    setTimeout(() => {
      for (let i = 0; i < 50000; i++) {
        let message = `Message ${i + 1}`;
        channel.sendToQueue(queueName, Buffer.from(message));
        console.log("Sent: " + message);
      }
    }, 1000);

    setTimeout(() => {
      for (let i = 0; i < 40000; i++) {
        let message = `Message ${i + 1}`;
        channel.sendToQueue(queueName, Buffer.from(message));
        console.log("Sent: " + message);
      }
    }, 5000);

    setTimeout(() => {
      connection.close();
    }, 6000);

  });
});
