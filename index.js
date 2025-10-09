import Fastify from 'fastify'
import cors from '@fastify/cors'
import mqtt from 'mqtt'
import sqlite3 from 'sqlite3'


// Initialize SQLite database
const dbPromise = new Promise((resolve, reject) => {
    const db = new sqlite3.Database('./hlr_db.db', (err) => {
        if (err) {
            return reject(err)
        }
        resolve(db)
    })
})

// Create a sample table if it doesn't exist
dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS iaq_sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        "device_id" TEXT,
        "temp" REAL,
        "humidity" REAL,
        "Co2" REAL,
        "type_label" TEXT,
        "mode" TEXT
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})


dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS order_device_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        "title" TEXT,
        "regen_fan_volt" REAL,
        "regen_heater_temp" REAL,
        "regen_duration" REAL,
        "scab_fan_volt" REAL,
        "scab_duration" REAL  
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})

// Connect to MQTT broker

const mqttClient = mqtt.connect('mqtt://broker.hivemq.com');

// MQTT event handlers
mqttClient.on('connect', () => {
    console.log('Connected to MQTT broker')
    mqttClient.subscribe('hlr-topic-test-demo', (err) => {
        if (!err) console.log('Subscribed to topic: mytopic')
    })
})

// Handle incoming messages
mqttClient.on('message', (topic, message) => {
    const jsonString = message.toString();
    try {
        const data = JSON.parse(jsonString);
        // Assuming the message contains device_id, temp, humidity, Co2, and type
        const { device_id, temp, humidity, Co2, type, mode } = data;

        // Insert the received data into the database
        dbPromise.then(async (db) => {
            const stmt = await db.prepare(`INSERT INTO iaq_sensor_data (device_id, temp, humidity, Co2, type_label, mode) VALUES (?, ?, ?, ?, ?, ?)`);
            await stmt.run(device_id, temp, humidity, Co2, type, mode);
            await stmt.finalize();
            // console.log('Data inserted into database:', data);
        }).catch(err => {
            console.error('Failed to insert data into database:', err);
        });
    } catch (e) {
        console.error('Failed to parse MQTT message as JSON:', e);
    }
})

const app = Fastify({
    logger: true
})

app.register(cors, {
    origin: '*'
})

app.get('/health', async (request, reply) => {
    return { status: 'ok' }
})

app.post('/data/iaq/', async (request, reply) => {
    const { start, end } = request.body;
    const db = await dbPromise
    const rows = await db.all('SELECT * FROM messages WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp DESC', [start, end])
    return rows
})

// Endpoint to publish a message to a topic
app.post('/publish', async (request, reply) => {
    const {
        topic,
        title,
        regen_fan_volt,
        regen_heater_temp,
        regen_duration,
        scab_fan_volt,
        scab_duration
    } = request.body
    const message = JSON.stringify({
        title,
        regen_fan_volt,
        regen_heater_temp,
        regen_duration,
        scab_fan_volt,
        scab_duration
    })

    // Save the command to the database
    const db = await dbPromise
    const stmt = await db.prepare(`INSERT INTO order_device_history (title, regen_fan_volt, regen_heater_temp, regen_duration, scab_fan_volt, scab_duration) VALUES (?, ?, ?, ?, ?, ?)`);
    await stmt.run(title, regen_fan_volt, regen_heater_temp, regen_duration, scab_fan_volt, scab_duration);
    await stmt.finalize();

    // Publish the message to the specified topic
    mqttClient.publish(topic, message)
    return { status: 'message published' }

})

app.listen({ port: 3011 }, (err, address) => {
    if (err) {
        app.log.error(err)
        process.exit(1)
    }
    app.log.info(`Server listening at ${address}`)
});