const express = require("express");
const { Kafka } = require("kafkajs");

const app = express();

const kafka = new Kafka({
  clientId: "api-gateway",
  brokers: ["localhost:9092"],
});

app.get("/", async (request, response) => {
  const producer = kafka.producer();

  await producer.connect();

  await producer.send({
    topic: "ECOMMERCE_NEW_ORDER",
    messages: [
      {
        key: "1",
        value: "mensagem nova",
      },
    ],
  });

  await producer.disconnect();

  response.send({
    data: "hello world 111111",
  });
});

const run = async () => {
  const consumer = kafka.consumer({ groupId: "GROUP_ECOMMERCE" });

  await consumer.connect();

  await consumer.subscribe({
    topic: "ECOMMERCE_ORDER_PAID",
    // fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ _, partition, message }) => {
      console.log("flow completed");
      console.log({
        partition,
        key: message.key.toString(),
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);

app.listen(3333, () => {
  console.log("estou ouvindo na porta 3333");
});
