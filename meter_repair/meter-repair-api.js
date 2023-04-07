const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'meter-repair'

module.exports = class MeterRepairApi {
	constructor(app) {
		//Получение списка счетчиков
		app.get(`/api/${ module_name }/meters`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Перепрограммирование счетчиков: запрос на информацию о счетчиках')
			
			const query = `select id, type, serial_number, port, ip_address, contact, prog_value
			                                            from meters where type in (23, 31, 33) order by id`
			
			executePGIQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/set-prog-value`, (apiReq, apiRes) => {
			const { error } = _validateProgVal(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			console.log(apiReq.body)
			if (!checkAuth(apiReq, apiRes))
				return
			
			const { id, value } = apiReq.body
			
			showRequestInfoAndTime('Перепрограммирование счетчиков: установка признака перепрограммирования')
			
			const query = `update meters set prog_value = ${ value } where id = ${ id } returning *`
			console.log(query)
			
			executePGIQuery(query, apiRes)
		})
	}
}

function _validateProgVal(meter) {
	const schema = {
		id: joi.number().required(),
		value: joi.number().required(),
	}
	return joi.validate(meter, schema);
}


function _validateCheckMeter(meter) {
	const schema = {
		serialNumber: joi.number().required(),
		type: joi.number().required(),
	}
	return joi.validate(meter, schema);
}