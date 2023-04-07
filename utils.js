const jwt = require('jsonwebtoken'),
joi = require('joi'),
tokenExp = '4h',
{ pgPool } = require("./database/postgres/postgres-db-connection")
const { getOraConnectionUit } = require('./database/oracle/oracle-db-connection.js')
const { authKey, roleKey } = require('./keys')

function _showRequestInfoAndTime(message) {
    const time = new Date().toLocaleTimeString('en-US', {
        hour12: false,
        hour: "numeric",
        minute: "numeric"
    });
    console.log(`${message} (время: ${time})\n`)
}

function _getDateTime() {
    const date = new Date()
    const year = date.getFullYear()
    const month = date.getMonth() + 1
    const day = date.getDate()
    const hours = date.getHours()
    const minutes = date.getMinutes()
    const seconds = date.getSeconds()
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
}

function _executePGIQuery(query, apiRes) {
    //console.log(query)
    pgPool.connect((connErr, client, done) => {
        if (connErr) apiRes.status(400).send(connErr.detail)
        
        client
            .query(query)
            .then(res => {
                done();
                //console.log('Запрос выполнен')
                apiRes.send(res.rows);
            })
            .catch(e => {
                done()
                console.log(`Запрос (${ query }). Ошибка: ${ e }`)
                apiRes.status(400).send(e.detail)
            })
    })
}


function _executeOraQuery(query, apiRes) {
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
    showRequestInfoAndTime: _showRequestInfoAndTime,
    getDateTime: _getDateTime,
    executePGIQuery: _executePGIQuery,
    executeOraQuery: _executeOraQuery,
    jwt: jwt,
    authKey: authKey,
    roleKey: roleKey,
    joi: joi,
    tokenExp: tokenExp
}