const pg = require('pg'),
{ postgresStekAsdDbConnection } = require('../../db-connections')

const pool = new pg.Pool(postgresStekAsdDbConnection)

module.exports = {
  pgStekASDPool: pool
}