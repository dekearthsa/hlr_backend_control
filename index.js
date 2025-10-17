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
    await db.exec(`CREATE TABLE IF NOT EXISTS setting_control (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        "cyclic_name" TEXT,
        "regen_fan_volt" REAL,
        "regen_heater_temp" REAL,
        "regen_duration" INTEGER,
        "scab_fan_volt" REAL,
        "scab_duration" INTEGER,
        "idle_duration" INTEGER,
        "cyclic_loop" INTEGER
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
    const { start, latesttime } = request.body;
    console.log(start, latesttime)
    const db = new Database('./hlr_db.db')
    // swr 
    // const db = new sqlite3.Database('/Users/pcsishun/project_envalic/hlr_control_system/hlr_backend/hlr_db.db')
    if (latesttime > 0) {
        // console.log("Aaaaa")
        // console.log("In f")
        const query = `
            SELECT id, datetime, sensor_id, co2, temperature, humidity, mode
            FROM hlr_sensor_data
            WHERE datetime > ?
            ORDER BY datetime ASC
            LIMIT 100
        `;
        const rows = db.prepare(query).all(latesttime)
        return rows;
    } else {
        // console.log("eee")
        const query = `SELECT * FROM hlr_sensor_data
        WHERE datetime >= ? ORDER BY datetime ASC
        `;
        const rows = db.prepare(query).all(start)
        console.log(rows)
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