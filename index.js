import Fastify from 'fastify'
import cors from '@fastify/cors'
import sqlite3 from 'sqlite3'
import Database from 'better-sqlite3';
// sqlite3.verbose()

// Initialize SQLite database
const dbPromise = new Promise((resolve, reject) => {
    const db = new sqlite3.Database('./hlr_db.db', (err) => {
        if (err) {
            return reject(err)
        }
        resolve(db)
    })
})


dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS hlr_sensor_data(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime INTEGER,
        sensor_id TEXT,
        co2 REAL,
        temperature REAL,
        humidity REAL,
        mode TEXT
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


const app = Fastify({
    logger: true
})

app.register(cors, {
    origin: '*'
})

app.get('/health', async (request, reply) => {
    return { status: 'ok' }
});


app.post('/loop/data/iaq', async (request, reply) => {
    const { start, lastTimestamp, firstTime } = request.body;
    console.log(start, lastTimestamp, firstTime)
    const db = new Database('./hlr_db.db')
    // swr 
    // const db = new sqlite3.Database('/Users/pcsishun/project_envalic/hlr_control_system/hlr_backend/hlr_db.db')
    if (!firstTime) {
        // console.log("In f")
        const query = `
            SELECT id, datetime, sensor_id, co2, temperature, humidity, mode
            FROM hlr_sensor_data
            WHERE datetime > ?
            ORDER BY datetime ASC
            LIMIT 100
        `;
        const rows = db.prepare(query).all(lastTimestamp)
        return rows;
    } else {
        const query = `SELECT * FROM hlr_sensor_data
        WHERE datetime >= ? ORDER BY datetime ASC
        `;
        const rows = db.prepare(query).all(start)
        return rows;
    }
});


app.listen({ port: 3011 }, (err, address) => {
    if (err) {
        app.log.error(err)
        process.exit(1)
    }
    app.log.info(`Server listening at ${address}`)
});