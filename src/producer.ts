import { Kafka, type Producer, type RecordMetadata } from 'kafkajs'; // grab the Kafka class from kafkajs
import { v4 as uuidv4 } from 'uuid';


type Order = {
 order_id: string
 user: string 
 item: string 
 quantity: number
 createdAt: string
}

// now we need to create a kafka instance and configure connection with kafka:
const kafka = new Kafka({
 clientId: 'task-producer',
 brokers: ['localhost:9092'], // this 9092 port is where kafka is accessible for clients
});




const producer : Producer  = kafka.producer(); // This creates the producer object. 
/**
 * What's producer? -> The "thing" that SENDS messages to Kafka.
 */


const USERS = ["Ashraf", "Sara", "Nadia", "Imran", "Fahim"];
const ITEMS = ["Macbook", "iPhone", "AirPods", "Keyboard", "Monitor"];

function randomInt( min:number, max:number) : number {
 return Math.floor( Math.random() * (max - min + 1)) + min; 
}

function pickOne(arr: string[]): string {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function generateOrder(): Order {
  return {
    order_id: uuidv4(),
    user: pickOne(USERS),
    item: pickOne(ITEMS),
    quantity: randomInt(1, 5),
    createdAt: new Date().toISOString(),
  };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Why async? -> Because talking to Kafka over network will take time and async will let us wait for responses without freezing.
async function main(){
  await producer.connect();
  /* 
  WHY await? -> Connecting takes time as it has to do real network work (talk to the Kafka broker, open a connection, negotiate metadata, etc). So, await says wait "don't continue until this finishes."
  Without await, the code would keep running before connection is ready, and we might try to send messages before the prodcuer is ready, which can cause errors/race conditions.
  */
  console.log("Producer connected")
  
  
  /*
   Since the order is a javascript object we need to convert it json string data type,
   because Kafka only sends strings( or bytes). So, use JSON.stringify(order) to convert!
  */
 let res: RecordMetadata[] | undefined;
  for(let i = 0; i < 200; i++){
    const order = generateOrder();

    res =  await producer.send({
     topic: "orders",
     acks: -1,
     messages: [
      {
       key: order.order_id,
       value: JSON.stringify(order),
      }
     ]
    });

    console.log(`Sent order -> ${i}`, `Customer: ${order.user} & OrderId: ${order.order_id}`);

    await sleep(100);
  } 
  console.log(res?.[0]?.baseOffset) // baseOffset is the first address of the batch.
  await producer.disconnect();
  console.log("Producer disconnected")

}


main().catch(console.error)