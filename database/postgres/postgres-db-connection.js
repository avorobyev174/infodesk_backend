const pg = require('pg'),
{ postgresInfodeskDbConnection } = require('../../db-connections')

const pool = new pg.Pool(postgresInfodeskDbConnection)

module.exports = {
  pgPool: pool
}