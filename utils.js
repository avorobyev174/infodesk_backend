const jwt = require('jsonwebtoken'),
joi = require('joi'),
tokenExp = '4h',
{ pgPool } = require("./database/postgres/postgres-db-connection")
const { getOraConnectionUit } = require('./database/oracle/oracle-db-connection.js')
const { authKey, roleKey } = require('./keys')

function showRequestInfoAndTime(message) {
    const time = new Date().toLocaleTimeString('en-US', {
        hour12: false,
        hour: "numeric",
        minute: "numeric"
    });
    console.log(`${ message } (время: ${ time })\n`)
}

function getDateTime() {
    const date = new Date()
    const year = date.getFullYear()
    const month = date.getMonth() + 1
    const day = date.getDate()
    const hours = date.getHours()
    const minutes = date.getMinutes()
    const seconds = date.getSeconds()
    return `${ year }-${ month }-${ day } ${ hours }:${ minutes }:${ seconds }`
}

async function executePGIQuery(query, apiRes) {
    //console.log(query)
    const client = await pgPool.connect()
    try {
        const { rows } = await client.query(query)
        apiRes.send(rows)
    } catch (e) {
        console.log(`Запрос (${ query }). Ошибка: ${ e }`)
        apiRes.status(400).send(e.detail)
    } finally {
        client.release()
    }
}


function executeOraQuery(query, apiRes) {
    getOraConnectionUit().then(
        oraConn => {
            oraConn.execute(query).then(
                result => {
                    oraConn.close()
                    apiRes.send(result.rows)
                },
                error => {
                    oraConn.close()
                    console.log(`Запрос (${ query }). Ошибка: ${ error }`);
                    apiRes.status(400).send(error.detail);
                }
            )
        }
    )
    
}


module.exports = {
    showRequestInfoAndTime,
    getDateTime,
    executePGIQuery,
    executeOraQuery,
    jwt,
    authKey,
    roleKey,
    joi,
    tokenExp
}