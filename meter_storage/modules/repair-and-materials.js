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
		
		app.get(`/api/${ module_name }/get-available-meters-from-repair/:type`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const meterType = apiReq.params.type
			const query = `select id, meter_type, serial_number, guid from meter where meter_location = 1
															${ meterType ? ' and meter_type = ' + meterType : '' }`
			
			const oraConn = await getOraConnectionUit()
			const { rows } = await oraConn.execute(query)
			
			const data = await Promise.all(rows.map(async (row) => {
				const queryLog =
					"select meter_log.update_field from meter_log " +
					"where meter_log.id = ( select max(ml.id)  " +
					"from meter m, meter_log ml " +
					"where m.meter_location = 1 " +
					`and m.id = ${ row.ID } ` +
					"and m.guid = ml.meter_guid " +
					"and ml.oper_type = 1)"
				
				const { rows } = await oraConn.execute(queryLog)
				const [ lastUpdateField ] = rows
				const updateField = lastUpdateField.UPDATE_FIELD
				return { ...row, updateField }
			}))
			
			return apiRes.send(data)
		})
		
		app.get(`/api/${ module_name }/get-available-meters-from-repair/`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
		
			const query = `select id, meter_type, serial_number, guid from meter where meter_location = 1`
			
			const oraConn = await getOraConnectionUit()
			const { rows } = await oraConn.execute(query)
			
			const data = await Promise.all(rows.map(async (row) => {
				const queryLog =
					"select meter_log.update_field from meter_log " +
					"where meter_log.id = ( select max(ml.id)  " +
					"from meter m, meter_log ml " +
					"where m.meter_location = 1 " +
					`and m.id = ${ row.ID } ` +
					"and m.guid = ml.meter_guid " +
					"and ml.oper_type = 1)"
				
				const { rows } = await oraConn.execute(queryLog)
				const [ lastUpdateField ] = rows
				const updateField = lastUpdateField.UPDATE_FIELD
				return { ...row, updateField }
			}))
			
			return apiRes.send(data)
		})
		
		app.post(`/api/${ module_name }/insert-meter-materials`, async (apiReq, apiRes) => {
			const { error } = _validateInsertMeterMaterials(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			console.log(apiReq.body)
			const { meters, materials, updateStr } = apiReq.body
			if (!meters.length) {
				return apiRes.status(400).send('Список счетчиков пустой')
			}
			if (!materials.length) {
				return apiRes.status(400).send('Список материалов пустой')
			}
			
			try {
				const oraConn = await getOraConnectionUit()
				for (const meter of meters) {
					const query = `select * from (select ml.id, ml.update_field
						from meter m, meter_log ml
						where m.serial_number = '${ meter.serialNumber }'
						and m.guid = ml.meter_guid and ml.oper_type = 1
						and m.meter_type = ${ meter.type } order by ml.id desc) where rownum = 1`
					
					console.log(query)
					const response = await oraConn.execute(query)
					console.log(response.rows)
					if (!response.rows.length) {
						return apiRes.status(400).send('не найден лог выдачи в ремонт')
					}
					
					const logId = response.rows[0].ID
					let updateField = response.rows[0].UPDATE_FIELD
					
					for (const material of materials) {
						console.log(material)
						const insertQuery = `insert into meter_spent_item (log_id, item_id, datetime, amount)
							values (${ logId },
							${ material.materialType },
							TO_DATE('${ getDateTime() }', 'yyyy--mm--dd hh24:mi:ss'),
							${ material.count })`
						console.log(insertQuery)
						const insertResponse = await oraConn.execute(insertQuery)
						console.log(insertResponse)
					}
					
					if (updateField) {
						updateField += updateStr.substring(24, updateField.length)
					} else {
						updateField = updateStr
					}
					const updateQuery = `update meter_log set update_field = '${ updateField }' where id = ${ logId }`
					console.log(updateQuery)
					const updateResponse = await oraConn.execute(updateQuery)
					console.log(updateResponse)
				}
				oraConn.close()
				return apiRes.send({ success: true })
			} catch ({ message }) {
				console.log(message)
				apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/insert-material-to-storage`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateStorageMaterials(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { materials } = apiReq.body
			if (!materials.length) {
				return apiRes.status(400).send('список материалов пустой')
			}
			
			try {
				const oraConn = await getOraConnectionUit()
				for (const material of materials) {
					console.log(material)
					const query = `insert into meter_item_storage (item_id, datetime, amount)
						values (${ material.materialType },
						TO_DATE('${ getDateTime() }', 'yyyy--mm--dd hh24:mi:ss'),
						${ material.count })`
					
					console.log(query)
					await oraConn.execute(query)
				}
				oraConn.close()
				return apiRes.send({ success: true })
			} catch ({ message }) {
				console.log(message)
				apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/insert-meter-work-status`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateInsertMeterWorkStatus(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { meters, isWorkable, comment, updateStr } = apiReq.body
			if (!meters.length) {
				return apiRes.status(400).send('список счетчиков пустой')
			}
			
			try {
				const oraConn = await getOraConnectionUit()
				for (const meter of meters) {
					const query = `select * from (select ml.id, ml.update_field
						from meter m, meter_log ml
						where m.serial_number = '${ meter.serialNumber }'
						and m.guid = ml.meter_guid and ml.oper_type = 1
						and m.meter_type = ${ meter.type } order by ml.id desc) where rownum = 1`
					
					console.log(query)
					const response = await oraConn.execute(query)
					console.log(response.rows)
					if (!response.rows.length) {
						return apiRes.status(400).send('не найден лог выдачи в ремонт')
					}
					
					const logId = response.rows[0].ID
					let updateField = response.rows[0].UPDATE_FIELD
					
					const insertQuery = `insert into meter_work_status (log_id, status, comment_field, datetime)
						values (${ logId },
						${ isWorkable ? 1 : 0 },
						'${ comment }',
						TO_DATE('${ getDateTime() }', 'yyyy--mm--dd hh24:mi:ss'))`
					console.log(insertQuery)
					const insertResponse = await oraConn.execute(insertQuery)
					console.log(insertResponse)
					if (updateField) {
						updateField += updateStr
					} else {
						updateField = updateStr
					}
			
					const updateQuery = `update meter_log set update_field = '${ updateField }' where id = ${ logId }`
					console.log(updateQuery)
					const updateResponse = await oraConn.execute(updateQuery)
					console.log(updateResponse)
				}
				oraConn.close()
				return apiRes.send({ success: true })
			} catch ({ message }) {
				console.log(message)
				apiRes.status(400).send(message)
			}
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

function _validateInsertMeterMaterials(meter) {
	const schema = {
		meters: joi.array().required(),
		materials: joi.array().required(),
		updateStr: joi.string().required()
	}
	return joi.validate(meter, schema);
}

function _validateInsertMeterWorkStatus(meter) {
	const schema = {
		meters: joi.array().required(),
		isWorkable: joi.boolean().required(),
		comment: joi.string().empty(''),
		updateStr: joi.string().required()
	}
	return joi.validate(meter, schema);
}

function _validateStorageMaterials(meter) {
	const schema = {
		materials: joi.array().required(),
	}
	return joi.validate(meter, schema);
}