import Fastify from 'fastify'
import cors from '@fastify/cors'
import sqlite3 from 'sqlite3'
import Database from 'better-sqlite3';
import axios from 'axios';
import { Parser } from 'json2csv'
import fs from 'fs'
import path from 'path'
// sqlite3.verbose()
const HTTP_API = 'http://172.29.247.180'

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
        mode TEXT,
        sensor_type TEXT,
        cyclicName TEXT
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})

dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS hlr_iaq_sensor_data(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        VOC REAL,
        CO2 REAL,
        CH2O REAL,
        eVOC REAL,
        Humid REAL,
        Temp REAL,
        PM25 REAL,
        PM10 REAL,
        CO REAL
    )`)
}).catch(err => {
    console.error('Failed to initialize database:', err)
})

dbPromise.then(async (db) => {
    await db.exec(`CREATE TABLE IF NOT EXISTS state_hlr (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        "cyclicName" TEXT,
        "systemState" TEXT,
        "systemType" TEXT,
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
    logger: {
        level: "error"
    }
});
app.register(cors, {
    origin: '*'
})



app.get('/health', async (request, reply) => {
    return { status: 'ok' }
});

app.post('/manual', async (request, reply) => {
    const { fanVolt, fanOn, heaterOn } = request.body;
    try {
        const payload = {
            fan_volt: fanOn ? fanVolt : 0,
            heater: heaterOn,
        }
        // console.log("manual = >", payload)
        const reusltOut = await axios.post(`${HTTP_API}/manual`, payload);
        if (reusltOut.status !== 200) {
            const db = new Database('./hlr_db.db');
            db.prepare("UPDATE state_hlr SET systemType = ?, systemState = ?;").run("manual", "manual");
            reply.send({ status: reusltOut.status })
        } else {
            reply.send({ status: 200 })
        }
    } catch (err) {
        console.log(`error in app.post('/manual' ${err}`)
        reply.send(err)
    }
});

// app.get("/stop")

app.get('/get/status', async (request, reply) => {
    // const { cyclicName } = request.body;
    // console.log("calling...")
    const db = new Database('./hlr_db.db');
    const sql = `SELECT * FROM state_hlr`;
    const row = db.prepare(sql).all();
    // console.log("row => ", row)
    reply.send(row)

})

app.post('/remove/format', async (request, reply) => {
    const { cyclicName } = request.body;
    // console.log(cyclicName)
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
                fanVolt: el.regen_fan_volt,
                heaterTemp: el.regen_heater_temp,
                durationMin: el.regen_duration,
            },
            scab: {
                fanVolt: el.scab_fan_volt,
                durationMin: el.scab_duration,
            },
            cool: {
                fanVolt: el.cool_fan,
                durationMin: el.cool_duration,
            },
            idle: {
                durationMin: el.idle_duration
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

app.get('/manual/stop', async (request, reply) => {
    const result = await axios.get(`${HTTP_API}/stop`);
    // console.log("/manual/stop => ", result)
    const db = new Database('./hlr_db.db');
    const queryStateHlr = ` UPDATE state_hlr SET
                    systemState = ?,
                    systemType = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?`;

    await db.prepare(queryStateHlr).run(
        "end",
        "",
        0,
        0,
        0,
        0,
    )
    if (result.status === 200) {
        reply.send("ok");
    } else {
        console.log(result.status)
    }

})

app.get('/stop', async (request, reply) => {
    // const { cyclicName } = request.body;
    const db = new Database('./hlr_db.db');

    // // production on this 
    // await axios.get(`http://localhost:3331/emergency_shutdown`)
    const queryStateHlr = ` UPDATE state_hlr SET
                    systemState = ?,
                    systemType = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?`;

    await db.prepare(queryStateHlr).run(
        "end",
        "",
        0,
        0,
        0,
        0,
    )

    // console.log(status)
    reply.send({ status: 'stopped' });
})


app.post('/start', async (request, reply) => {
    const {
        cyclicName,
        systemType,
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
                    systemType = ?,
                    systemState = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?
                `;

                await db.prepare(queryStateHlr).run(
                    cyclicName,
                    systemType,
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
                    (cyclicName, systemType,systemState, is_start, cyclic_loop_dur, starttime, endtime)
                    VALUES (?,?,?,?,?,?,?)
                `;
                db.prepare(queryStateHlr).run(cyclicName, systemType, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
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
                    systemType = ?,
                    systemState = ?,
                    is_start = ?,
                    cyclic_loop_dur = ?,
                    starttime = ?,
                    endtime = ?
                `;
                db.prepare(queryStateHlr).run(cyclicName, systemType, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
                reply.send({ status: 'inserted', cyclicName });
            } else {
                const queryStateHlr = ` INSERT INTO state_hlr
                    (cyclicName, systemType,cyclicName, systemState, is_start, cyclic_loop_dur, starttime, endtime)
                    VALUES (?,?,?, ?,?,?)
                `;
                db.prepare(queryStateHlr).run(cyclicName, systemType, "regen_firsttime", 1, cyclicLoop, ms, ms + (Number(regenDur) * 60 * 1000))
                reply.send({ status: 'inserted', cyclicName });
            }


        }
    } catch (err) {
        console.error('DB Error:', err);
        reply.status(500).send({ error: 'Database operation failed', detail: err.message });
    }
});

app.post("/receive/iaq", async (request, reply) => {
    const payloadIN = request.body;
    // const payloadIN = json.parse(raw);
    // print(payloadIN)
    // console.log("payloadIN => ", payloadIN)
    if (!payloadIN.data.is_updated) return reply.status(200).send("is_updated: false");
    const db = new Database('./hlr_db.db')
    const ms = Date.now();
    const query = `INSERT INTO hlr_iaq_sensor_data
            (timestamp,VOC,CO2,CH2O,eVOC,Humid, Temp, PM25, PM10, CO)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    db.prepare(query).run(
        ms, //1 
        payloadIN.data.VOC_ppb, // voc 2
        payloadIN.data.CO2_ppm, // co2 3
        payloadIN.data.CH2O_ppm, // ch20 4
        payloadIN.data.eVOC_ppb, // eboc 5
        0, // 6
        0, // 7
        0, // 8
        0, // 9
        0 // 10
    );

    reply.status(200).send("ok")
})

app.post("/download/iaq/csv", async (request, reply) => {
    const { startMs, endMs } = request.body;
    if (!startMs) return reply.status(400).send("Invalid payload");
    if (!endMs) return reply.status(400).send("Invalid payload");
    const db = new Database('./hlr_db.db')
    const query = `
                SELECT
                    strftime('%Y-%m-%d %H:%M:00', timestamp/1000, 'unixepoch', '+7 hours') AS minute_th,
                    AVG(VOC) AS VOC,
                    AVG(CO2) AS CO2,
                    AVG(CH2O) AS CH2O,
                    AVG(eVOC) AS eVOC,
                    AVG(Humid) AS Humid,
                    AVG(Temp) AS Temp,
                    AVG(PM25) AS PM25,
                    AVG(PM10) AS PM10,
                    AVG(CO) AS CO
                FROM hlr_iaq_sensor_data
                WHERE timestamp BETWEEN ? AND ?
                GROUP BY minute_th;
                `
    const rows = db.prepare(query).all(startMs, endMs);
    // console.log(rows);
    // --- ใช้ json2csv แปลงเป็นไฟล์ CSV ---
    const parser = new Parser({
        fields: [
            'minute_th',
            'VOC',
            'CO2',
            'CH2O',
            'eVOC',
            'Humid',
            'Temp',
            'PM25',
            'PM10',
            'CO'
        ]
    });
    const csv = parser.parse(rows);

    // --- ตั้งชื่อไฟล์และเขียนชั่วคราว ---
    const filename = `sensor_iaq_avg_1min_${Date.now()}.csv`;
    const filepath = path.join('./', filename);
    fs.writeFileSync(filepath, csv);

    // --- ส่งออกเป็นไฟล์ให้โหลด ---
    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
    // return reply.status(200).send(rows)
})

app.post("/download/csv", async (request, reply) => {
    const { startMs, endMs } = request.body;
    if (!startMs) return reply.status(400).send("Invalid payload");
    if (!endMs) return reply.status(400).send("Invalid payload");
    const db = new Database('./hlr_db.db')
    const query = `SELECT
                minute_th,
                cyclicName,

                -- CO2
                MAX(CASE WHEN sensor_id = 2  THEN avg_co2_adjust END) AS co2_outlet,
                MAX(CASE WHEN sensor_id = 3  THEN avg_co2_adjust END) AS co2_inlet,

                -- Temperature
                MAX(CASE WHEN sensor_id = 2  THEN avg_temperature END) AS temp_outlet,
                MAX(CASE WHEN sensor_id = 3  THEN avg_temperature END) AS temp_inlet,
                MAX(CASE WHEN sensor_id = 51 THEN avg_temperature END) AS temp_tk,

                -- Humidity
                MAX(CASE WHEN sensor_id = 2  THEN avg_humidity END)   AS humid_outlet,
                MAX(CASE WHEN sensor_id = 3  THEN avg_humidity END)   AS humid_inlet
                FROM (
                SELECT
                    sensor_id,
                    cyclicName,
                    strftime('%Y-%m-%d %H:%M:00', datetime/1000, 'unixepoch', '+7 hours') AS minute_th,

                    AVG(co2)         AS avg_co2,
                    AVG(temperature) AS avg_temperature,
                    AVG(humidity)    AS avg_humidity,

                    AVG(
                    CASE
                        WHEN sensor_id = 2  THEN (1.023672650 * co2) - 19.479471
                        WHEN sensor_id = 3  THEN (0.970384222 * co2) - 99.184335
                        WHEN sensor_id = 51 THEN 0
                    END
                    ) AS avg_co2_adjust

                FROM hlr_sensor_data
                WHERE datetime BETWEEN ? AND ?
                GROUP BY sensor_id, minute_th, cyclicName
                )
                GROUP BY minute_th, cyclicName
                ORDER BY minute_th;
                `
    const rows = db.prepare(query).all(startMs, endMs);
    // console.log(rows);
    // --- ใช้ json2csv แปลงเป็นไฟล์ CSV ---
    const parser = new Parser({
        fields: [
            'minute_th',
            'cyclicName',
            'co2_outlet',
            'co2_inlet',
            'temp_outlet',
            'temp_inlet',
            'temp_tk',
            'humid_outlet',
            'humid_inlet'
        ]
    });
    const csv = parser.parse(rows);

    // --- ตั้งชื่อไฟล์และเขียนชั่วคราว ---
    const filename = `sensor_avg_1min_${Date.now()}.csv`;
    const filepath = path.join('./', filename);
    fs.writeFileSync(filepath, csv);

    // --- ส่งออกเป็นไฟล์ให้โหลด ---
    reply.header('Content-Type', 'text/csv');
    reply.header('Content-Disposition', `attachment; filename="${filename}"`);
    return reply.send(fs.createReadStream(filepath));
    // return reply.status(200).send(rows)
})

// app.post("/get")

app.post('/loop/data/iaq', async (request, reply) => {
    const { start, latesttime } = request.body;
    // console.log(start, latesttime)
    const db = new Database('./hlr_db.db');
    // swr 
    // const db = new sqlite3.Database('/Users/pcsishun/project_envalic/hlr_control_system/hlr_backend/hlr_db.db')
    if (latesttime > 0) {
        const query = `
            SELECT datetime, sensor_id, temperature, humidity, mode,
            CASE
                WHEN sensor_id = 2 THEN (1.023672650 * co2) - 19.479471
                WHEN sensor_id = 3 THEN (0.970384222 * co2)- 99.184335
                WHEN sensor_id = 51 THEN 0
            END co2
            FROM hlr_sensor_data
            WHERE datetime > ?
            ORDER BY datetime ASC
            LIMIT 100
        `;
        const rows = db.prepare(query).all(latesttime)
        return rows;
    } else {
        // console.log("eee")
        const query = `SELECT datetime,sensor_id, 
            CASE
                WHEN sensor_id = 2 THEN (1.023672650 * co2) - 19.479471
                WHEN sensor_id = 3 THEN (0.970384222 * co2)- 99.184335
                WHEN sensor_id = 51 THEN 0
            END co2, temperature, humidity, mode
            FROM hlr_sensor_data
        WHERE datetime >= ?  ORDER BY datetime ASC
        `;
        const rows = db.prepare(query).all(start)
        // console.log(rows)
        return rows;
    }
});


app.listen({ port: 3011, host: '0.0.0.0' }, (err, address) => {
    if (err) {
        app.log.error(err)
        process.exit(1)
    }
    console.log(`Service hlr-backend run at 3011`)
    // app.log.info(`Server listening at ${address}`)
});