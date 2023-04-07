const pg = require('pg'),
{ postgresStekDbConnection } = require('../../db-connections')

const pool = new pg.Pool(postgresStekDbConnection)

module.exports = {
  pgStekPool: pool
}