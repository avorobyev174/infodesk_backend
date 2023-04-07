const { showRequestInfoAndTime, joi, getDateTime, executeOraQuery  } = require('../../utils'),
{ getOraConnectionUit } = require("../../database/oracle/oracle-db-connection"),
{ checkAuth } = require('../../login/login-api'),
oracledb = require('oracledb')

module.exports = class repairAndMaterials {
	constructor(app, module_name) {
		app.get(`/api/${ module_name }/materials-types`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на типы счетчиков')
			
			const query = `select * from meter_item order by item`
			
			executeOraQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/get-meter-types-in-repair`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на типы счетчиков')
			
			const query = `select m.meter_type
							from meter m, meter_log l
							where m.meter_location = 1
							and m.guid = l.meter_guid group by m.meter_type`
			
			executeOraQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/check-meter-in-repair`, (apiReq, apiRes) => {
			
			const { error } = _validateCheckMeter(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { serialNumber, type } = apiReq.body
			
			showRequestInfoAndTime('Склад счетчиков: запрос на получение счетчика по type и serialNumber в ремонте')
			
			const query = `select * from meter where meter_type = ${ type }
										and serial_number = '${ serialNumber }'
										and meter_location = 1`
			
			executeOraQuery(query, apiRes)
		})
		
	}
}

function _validateCheckMeter(meter) {
	const schema = {
		serialNumber: joi.string().required(),
		type: joi.number().required()
	}
	return joi.validate(meter, schema);
}