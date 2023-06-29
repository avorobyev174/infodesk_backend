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
    let month = date.getMonth() + 1
    let day = date.getDate()
    if (month < 10) month = '0' + month
    if (day < 10) day = '0' + day
    
    let hours = date.getHours()
    let minutes = date.getMinutes()
    let seconds = date.getSeconds()
    if (hours < 10) hours = '0' + hours
    if (minutes < 10) minutes = '0' + minutes
    if (seconds < 10) seconds = '0' + seconds
    
    return `${ year }-${ month }-${ day } ${ hours }:${ minutes }:${ seconds }`
}

function formatDateTimeForUser(datetime, withTime) {
    const date = new Date(datetime)
    const year = date.getFullYear()
    let month = date.getMonth() + 1
    let day = date.getDate()
    if (month < 10) month = '0' + month
    if (day < 10) day = '0' + day
    
    if (withTime) {
        let hours = date.getHours()
        let minutes = date.getMinutes()
        let seconds = date.getSeconds()
        if (hours < 10) hours = '0' + hours
        if (minutes < 10) minutes = '0' + minutes
        if (seconds < 10) seconds = '0' + seconds
        
        return `${ day }.${ month }.${ year } ${ hours }:${ minutes }:${ seconds }`
    }
    
    return `${ day }.${ month }.${ year }`
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
    executeQuery,
    formatDateTimeForUser
}