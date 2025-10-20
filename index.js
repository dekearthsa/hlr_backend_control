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
    await db.exec(`CREATE TABLE IF NOT EXISTS state_hlr (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        "cyclicName" TEXT,
        "systemState" TEXT,
        "is_start" INTEGER,
        "cyclic_loop_dur" INTEGER,
        "starttime" INTEGER,
        "endtime" INTEGER
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
        "cool_fan" REAL,
        "cool_duration" INTEGER,
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

app.post('/get/status', async (request, reply) => {
    const { cyclicName } = request.body;
    const db = new Database('./hlr_db.db');
    const sql = `SELECT * FROM state_hlr WHERE cyclicName = ?`

})

app.post('/remove/format', async (request, reply) => {
    const { cyclicName } = request.body;
    console.log(cyclicName)
    const db = new Database('./hlr_db.db');
    const sqlRmove = `DELETE FROM setting_control WHERE cyclic_name = ?`
    const sqlStateRemove = `DELETE FROM state_hlr WHERE cyclicName = ?`
    const deleteStatement = db.prepare(sqlRmove);
    const deleteStatementState = db.prepare(sqlStateRemove);
    await deleteStatement.run(cyclicName);
    await deleteStatementState.run(cyclicName)
    reply.send({ status: "deleted", desc: "Remove " + cyclicName })
});

app.get('/push/format', async (request, reply) => {
    const db = new Database('./hlr_db.db');
    const sqlGet = `SELECT * FROM setting_control`
    const rows = db.prepare(sqlGet).all();
    // console.log("rows= >", rows)
    const arrayFormat = []
    for (const el of rows) {
        const setFormat = {
            id: el.id,
            title: el.cyclic_name,
            regen: {
                fanVolt: 0,
                heaterTemp: 0,
                durationMin: 0,
            },
            scab: {
                fanVolt: 0,
                durationMin: 0,
            },
            cool: {
                fanVolt: 0,
                durationMin: 0,
            },
            idle: {
                durationMin: 0
            },
            savedAt: el.timestamp,
            cyclic_loop: el.cyclic_loop
        }
        arrayFormat.push(setFormat)
    }
    // console.log("arrayFormat => ", arrayFormat)
    reply.send(arrayFormat)
})

app.post("/save/format", async (request, reply) => {
    const {
        cyclicName,
        regenFan,
        regenHeater,
        regenDur, // int min
        scabFan,
        scabDur, // int min
        coolFan,
        coolDur, // int min
        idelDur, // int min
        cyclicLoop,
    } = request.body;
    const ms = Date.now();
    const db = new Database('./hlr_db.db');
    const queryInsert = `
                INSERT INTO setting_control 
                (timestamp, cyclic_name, regen_fan_volt, regen_heater_temp, regen_duration, scab_fan_volt, scab_duration, cool_fan, cool_duration, idle_duration, cyclic_loop)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;

    db.prepare(queryInsert).run(
        ms,
        cyclicName,
        regenFan,
        regenHeater,
        regenDur,
        scabFan,
        scabDur,
        coolFan,
        coolDur,
        idelDur,
        cyclicLoop
    );
    reply.send({ status: 'inserted', cyclicName });
})

app.post('/stop', async (request, reply) => {
    const { cyclicName } = request.body;
    const db = new Database('./hlr_db.db');
    // const queryFindStateHlr = `SELECT * FROM state_hlr`;
    const queryStateHlr = ` UPDATE state_hlr
                    SET
                    systemState = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?
                    WHERE cyclicName = ?
                `;

    await db.prepare(queryStateHlr).run(
        "end",
        0,
        0,
        0,
        0,
        cyclicName
    )

    // console.log(status)
    reply.send({ status: 'stopped', cyclicName });
})


app.post('/start', async (request, reply) => {
    const {
        cyclicName,
        regenFan,
        regenHeater,
        regenDur, // int min
        scabFan,
        scabDur, // int min
        coolFan,
        coolDur, // int min
        idelDur, // int min
        cyclicLoop,
    } = request.body;
    // console.log(cyclicName, regenFan, regenHeater, regenDur, scabFan, scabDur, coolFan, coolDur, idelDur, cyclicLoop)
    const ms = Date.now();

    try {
        const db = new Database('./hlr_db.db')
        const queryFind = `SELECT * FROM setting_control WHERE cyclic_name = ?`;
        const queryFindStateHlr = `SELECT * FROM state_hlr`;
        const rows = db.prepare(queryFind).all(cyclicName);
        const rowsStateHlr = db.prepare(queryFindStateHlr).all();
        // console.log("rows => ", rows)
        // console.log("rowsStateHlr =s> ", rowsStateHlr)
        if (rows.length > 0) {
            // Update ถ้ามีข้อมูลอยู่แล้ว
            // console.log("(rows.length > 0) {")
            const queryUpdate = `
                UPDATE setting_control
                SET 
                timestamp = ?,
                regen_fan_volt = ?,
                regen_heater_temp = ?,
                regen_duration = ?,
                scab_fan_volt = ?,
                scab_duration = ?,
                cool_fan = ?,
                cool_duration = ?,
                idle_duration = ?,
                cyclic_loop = ?
                WHERE cyclic_name = ?
            `;

            db.prepare(queryUpdate).run(
                ms,
                regenFan,
                regenHeater,
                regenDur,
                scabFan,
                scabDur,
                coolFan,
                coolDur,
                idelDur,
                cyclicLoop,
                cyclicName
            );

            if (rowsStateHlr.length > 0) {
                const queryStateHlr = ` UPDATE state_hlr
                    SET
                    cyclicName = ?,
                    systemState = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?
                `;

                await db.prepare(queryStateHlr).run(
                    cyclicName,
                    "regen_firsttime",
                    1,
                    cyclicLoop,
                    ms,
                    ms + (Number(regenDur) * 60 * 1000)
                )

                // console.log("update...")
                reply.send({ status: 'updated', cyclicName });
            } else {
                // console.log(ms)
                // console.log(ms + (regenDur * 60 * 1000))
                const queryStateHlr = ` INSERT INTO state_hlr
                    (cyclicName, systemState, is_start, cyclic_loop_dur, starttime, endtime)
                    VALUES (?,?,?,?,?,?)
                `;
                db.prepare(queryStateHlr).run(cyclicName, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
                reply.send({ status: 'updated', cyclicName });
            }

        } else {
            // Insert ถ้ายังไม่มีข้อมูล
            const queryInsert = `
                INSERT INTO setting_control 
                (timestamp, cyclic_name, regen_fan_volt, regen_heater_temp, regen_duration, scab_fan_volt, scab_duration, cool_fan, cool_duration, idle_duration, cyclic_loop)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `;

            db.prepare(queryInsert).run(
                ms,
                cyclicName,
                regenFan,
                regenHeater,
                regenDur,
                scabFan,
                scabDur,
                coolFan,
                coolDur,
                idelDur,
                cyclicLoop
            );

            if (rowsStateHlr.length > 0) {
                const queryStateHlr = ` UPDATE state_hlr
                    SET
                    cyclicName = ?,
                    systemState = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?
                `;
                db.prepare(queryStateHlr).run(cyclicName, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
                reply.send({ status: 'inserted', cyclicName });
            } else {
                const queryStateHlr = ` INSERT INTO state_hlr
                    (cyclicName,cyclicName, systemState, is_start, cyclic_loop_dur, starttime, endtime)
                    VALUES (?,?, ?,?,?)
                `;
                db.prepare(queryStateHlr).run(cyclicName, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
                reply.send({ status: 'inserted', cyclicName });
            }


        }
    } catch (err) {
        console.error('DB Error:', err);
        reply.status(500).send({ error: 'Database operation failed', detail: err.message });
    }
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