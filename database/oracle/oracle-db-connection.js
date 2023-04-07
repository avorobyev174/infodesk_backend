const oracledb = require('oracledb'),
{ oracleCntDbConnection, oracleUitDbConnection } = require('../../db-connections')

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT
oracledb.autoCommit = true

async function getConnectionCnt() {
  return await oracledb.getConnection(oracleCntDbConnection)
}

async function getConnectionUit() {
  return await oracledb.getConnection(oracleUitDbConnection)
}

module.exports = {
  getOraConnectionCnt: getConnectionCnt,
  getOraConnectionUit: getConnectionUit
}