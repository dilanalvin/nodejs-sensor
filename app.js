const mqtt = require('mqtt');
const WebSocket = require('ws');
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const { MongoClient, ObjectId } = require('mongodb');

// MQTT Server Details
const mqttServer = "mqtt://mqtt.iot.asmat.app";
const mqttClient = mqtt.connect(mqttServer, {
  username: "SENSOR_KURSI_IGNITE",
  password: "",
});

// WebSocket Server
const wss = new WebSocket.Server({ port: 8080 });

// MongoDB Configuration
// const mongoUri = "mongodb://localhost:27017";
const mongoUri = "mongodb+srv://ramadhanrizqi21:T0cXKXHl5TIGokai@skripsicluster.vrfa9mf.mongodb.net/?retryWrites=true&w=majority&appName=SkripsiCluster";

const dbName = "iot_sensor";
const collectionName = "raw_data_sensor";

// Create MongoDB Client
const mongoClient = new MongoClient(mongoUri);

// Express App
const app = express();
const port = 3000;

app.use(bodyParser.json());
app.use(cors());
let db, collection;

async function run() {
  try {
    // Connect to MongoDB
    await mongoClient.connect();
    console.log("Connected successfully to MongoDB");

    db = mongoClient.db(dbName);
    collection = db.collection(collectionName);

    // MQTT Client Setup
    mqttClient.on('connect', () => {
      console.log('Connected to MQTT broker');
      mqttClient.subscribe('Sensor_Kursi_Alvin', (err) => {
        if (err) {
          console.error('Failed to subscribe to topic', err);
        }
      });
    });

    mqttClient.on('message', async (topic, message) => {
      const msgString = message.toString();
      console.log(`Received message: ${msgString} from topic: ${topic}`);

      // Save message to MongoDB
      try {
        const messageObject = JSON.parse(message);
        let ts = Date.now();
        const result = await collection.insertOne({
          topic,
          message: msgString,
          time_sensor: messageObject.time,
          data_sensor: messageObject.sensor,
          timestamp:ts,
        });
        console.log(`data message ${messageObject.time}`);
        // console.log('Message saved to MongoDB with id:', result.insertedId);
      } catch (err) {
        console.error('Failed to save message to MongoDB', err);
      }

      // Broadcast message to WebSocket clients
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(msgString);
        }
      });
    });

    wss.on('connection', (ws) => {
      console.log('WebSocket client connected');
    });

    console.log('WebSocket server started on ws://localhost:8080');

  } catch (err) {
    console.error('Error connecting to MongoDB', err);
  }
}

// Function to group data by time_sensor with pagination
async function groupDataByTimeSensor(page, limit) {
  try {
    const pipeline = [
      {
        $group: {
          _id: "$time_sensor",
          data: { $push: "$$ROOT" }
        }
      },
      {
        $project: {
          _id: 0,
          time_sensor: "$_id",
          data: 1
        }
      },
      {
        $sort: { timestamp: -1 } // Urutkan berdasarkan time_sensor secara menurun
      },
      {
        $skip: (page - 1) * limit
      },
      {
        $limit: limit
      }
    ];

    const groupedData = await collection.aggregate(pipeline).toArray();

    // Mendapatkan total count dokumen untuk informasi pagination
    const totalCount = await collection.aggregate([
      {
        $group: {
          _id: "$time_sensor"
        }
      },
      {
        $count: "count"
      }
    ]).toArray();

    const totalPages = Math.ceil(totalCount[0].count / limit);

    return {
      totalPages,
      currentPage: page,
      data: groupedData
    };

  } catch (err) {
    console.error('Error grouping data', err);
    return {
      totalPages: 0,
      currentPage: page,
      data: []
    };
  }
}

// API Routes
app.get('/', (req, res) => {
  res.send('Hello World!');
});

// Create a new sensor data
app.post('/sensor', async (req, res) => {
  try {
    const sensorData = req.body;
    const result = await collection.insertOne(sensorData);
    res.status(201).send(result.ops[0]);
  } catch (error) {
    res.status(400).send(error);
  }
});

// Get all sensor data with pagination
app.get('/sensor', async (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 10;

  try {
    const result = await groupDataByTimeSensor(page, limit);
    res.status(200).send({
      "status":200,
      "message":"success",
      "result":result
    });
  } catch (error) {
    res.status(500).send(error);
  }
});

// Get a single sensor data by ID
app.get('/sensor/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const sensorData = await collection.findOne({ _id: new ObjectId(id) });
    if (!sensorData) {
      return res.status(404).send();
    }
    res.status(200).send(sensorData);
  } catch (error) {
    res.status(500).send(error);
  }
});

// Update a sensor data by ID
app.patch('/sensor/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const updates = req.body;
    const result = await collection.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnOriginal: false }
    );
    if (!result.value) {
      return res.status(404).send();
    }
    res.status(200).send(result.value);
  } catch (error) {
    res.status(400).send(error);
  }
});

// Delete a sensor data by ID
app.delete('/sensor/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const result = await collection.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).send();
    }
    res.status(200).send({ message: 'Sensor data deleted' });
  } catch (error) {
    res.status(500).send(error);
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

// Run the setup
run().catch(console.dir);

// Close MongoDB client on process exit
process.on('SIGINT', async () => {
  await mongoClient.close();
  process.exit(0);
});
