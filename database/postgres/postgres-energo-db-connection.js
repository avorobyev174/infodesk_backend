const pg = require('pg'),
{ postgresEnergoDbConnection } = require('../../db-connections')

const pool = new pg.Pool(postgresEnergoDbConnection)

module.exports = {
  pgEnergoPool: pool
}