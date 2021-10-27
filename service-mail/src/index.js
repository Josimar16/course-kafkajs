const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "service-mail",
  brokers: ["localhost:9092"],
});

const run = async () => {
  const consumer = kafka.consumer({ groupId: "GROUP_SERVICE_MAIL" });

  await consumer.connect();

  await consumer.subscribe({
    topic: "ECOMMERCE_NEW_ORDER",
    // fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ _, partition, message }) => {
      console.log({
        partition,
        key: message.key.toString(),
        value: message.value.toString(),
      });

      console.log("---- produzindo um novo topico ----");

      const producer = kafka.producer();

      await producer.connect();

      await producer.send({
        topic: "ECOMMERCE_SEND_MAIL",
        messages: [{ key: "11111", value: "mail enviado" }],
      });

      await producer.disconnect();
    },
  });
};

run().catch(console.error);
