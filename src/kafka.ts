import { Kafka, type Consumer, type Producer } from 'kafkajs';

// now we need to create a kafka instance and configure connection with kafka:
const kafka = new Kafka({
  clientId: 'task-consumer',
  brokers: ['localhost:9092'], // this 9092 port is where kafka is accessible for clients
});

// unlike producers, in consumers we must specify which group our consumer belongs into by passing a ConsumerConfig argument( which is an object with certain instructions for the consumer which includes groupId ) and we do that by giving groupId.
// so that groupId tells which team of workers is this specific consumer part of.
// by doing this, kafka gets the information needed to manage offsets and co-ordinate partition assignment.

export const consumer: Consumer = kafka.consumer({
  groupId: 'order-tracker',
});

// DLQ producer — sends permanently failed orders to the dead-letter topic
export const dlqProducer: Producer = kafka.producer();
