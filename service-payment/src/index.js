const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "service-payment",
  brokers: ["localhost:9092"],
});

const run = async () => {
  const consumer = kafka.consumer({ groupId: "GROUP_SERVICE_PAYMENT" });

  await consumer.connect();

  await consumer.subscribe({
    topic: "ECOMMERCE_SEND_MAIL",
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
        topic: "ECOMMERCE_ORDER_PAID",
        messages: [{ key: "555", value: "PRODUTO FOI PAGO" }],
      });

      await producer.disconnect();

      console.log("---- topico produzido ----");
    },
  });
};

run().catch(console.error);
