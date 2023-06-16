const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'common'

module.exports = class CommonApi {
	constructor(app) {
		//Получение списка счетчиков
		app.get(`/api/${ module_name }/accounts`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			executePGIQuery('select id, full_name from accounts order by id', apiRes)
		})
		
	}
}
