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

function getCurrentDateTime() {
    const date = new Date()
    const year = date.getFullYear()
    const month = date.getMonth() + 1
    const day = date.getDate()
    const hours = date.getHours()
    const minutes = date.getMinutes()
    const seconds = date.getSeconds()
    return `${ year }-${ month }-${ day } ${ hours }:${ minutes }:${ seconds }`
}

function formatDateTime(datetime) {
    const date = new Date(datetime)
    const year = date.getFullYear()
    const month = date.getMonth() + 1
    const day = date.getDate()
    const hours = date.getHours()
    const minutes = date.getMinutes()
    const seconds = date.getSeconds()
    return `${ year }-${ month }-${ day } ${ hours }:${ minutes }:${ seconds }`
}

async function executePGIQuery(query, apiRes) {
    const client = await pgPool.connect()
    try {
        const { rows } = await client.query(query)
        apiRes.send(rows)
    } catch (e) {
        apiRes.status(400).send(e.detail || e.message)
    } finally {
        client.release()
    }
}

async function executeQuery(client, query) {
    const { rows } = await client.query(query)
    return rows
}


module.exports = {
    showRequestInfoAndTime,
    getCurrentDateTime,
    formatDateTime,
    executePGIQuery,
    jwt,
    authKey,
    roleKey,
    joi,
    tokenExp,
    executeQuery
}