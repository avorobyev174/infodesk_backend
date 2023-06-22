const { showRequestInfoAndTime, joi, getCurrentDateTime, executePGIQuery  } = require('../../utils'),
{ pgPool } = require("../../database/postgres/postgres-db-connection"),
{ checkAuth } = require('../../login/login-api')

module.exports = class acceptOrIssueApi {
	constructor(app, module_name) {
		const REPAIR_OPERATION = 1
		const REGISTER_OPERATION = 7
		const STORAGE_LOCATION = 0
		const REPAIR_LOCATION = 1
		const SECOND_STORAGE_LOCATION = 7
		
		app.post(`/api/${ module_name }/check-meter`, (apiReq, apiRes) => {
			
			const { error } = _validateCheckMeter(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
				
			const { serialNumber, type } = apiReq.body
			
			showRequestInfoAndTime('Склад счетчиков: запрос на получение счетчика по type и serialNumber')
			
			const query = `select * from meter where meter_type = ${ type } and serial_number = '${ serialNumber }'`
			
			executePGIQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/registration`, async (apiReq, apiRes) => {
			const { error } = _validateRegisterMeter(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const {
				meters,
				acceptedPersonStaffId,
				accuracyClass,
				condition,
				issuingPersonStaffId,
				storageType,
				interval,
				owner,
				calibration,
				passportNumber,
				comment,
				isRouter
			} = apiReq.body
			
			const calibrationDate = !calibration ? null : `to_date('${ calibration }', 'DD.MM.YYYY')`
			const operationType = isRouter ? REPAIR_OPERATION : REGISTER_OPERATION
			const meterLocation = isRouter
				? REPAIR_LOCATION
				: storageType ? SECOND_STORAGE_LOCATION : STORAGE_LOCATION
			
			const client = await pgPool.connect()
			const results = []
			try {
				for (const meter of meters) {
					const guid = generateGuid()
					let query = "insert into meter (meter_type, serial_number, guid, accuracy_class, condition, passport_number, " +
						"calibration_date, calibration_interval, meter_location, current_owner, property, lv_modem) " +
						`values (
	                            ${ meter.type },
	                            '${ meter.serialNumber }',
	                            '${ guid }',
	                            ${ accuracyClass },
	                            ${ condition },
	                            ${ passportNumber },
	                            ${ calibrationDate },
	                            ${ interval },
	                            ${ meterLocation },
	                            ${ acceptedPersonStaffId },
	                            ${ owner },
	                            0) returning *`
					
					let queryLog = "insert into meter_log (meter_guid, meter_serial_number, oper_type, issuing_person, " +
						"accepted_person, datetime, old_location, new_location, comment_field) " +
						`values (
	                                '${ guid }',
	                                '${ meter.serialNumber }',
	                                 ${ operationType },
	                                 ${ issuingPersonStaffId },
	                                 ${ acceptedPersonStaffId },
	                                 '${ getCurrentDateTime() }',
	                                 null,
	                                 ${ meterLocation },
	                                 '${ comment }')`
					
					const { rows } = await client.query(query)
					const [ insertedMeter ] = rows
					await client.query(queryLog)
					results.push({ ...insertedMeter, success: true })
				}
				
				apiRes.send(results)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(`ошибка при регистрации ${ e.message || e.detail }`)
			} finally {
				client.release()
			}
		})
		
		app.post(`/api/${ module_name }/edit`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateEditMeter(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const {
				type,
				serialNumber,
				editorStaffId,
				accuracyClass,
				condition,
				interval,
				calibration,
				comment,
				passportNumber,
				guid,
				updateField
			} = apiReq.body
			
			const calibrationDate = !calibration ? null : `to_date('${ calibration }', 'DD.MM.YYYY')`
			const commentLog = comment ? comment : ''
			const passportNum = passportNumber ? passportNumber : 0
			
			const client = await pgPool.connect()
			try {
				let query = `update meter set meter_type = ${ type }, serial_number = ${ serialNumber },
								accuracy_class = ${ accuracyClass }, condition = ${ condition },
								calibration_date = ${ calibrationDate }, passport_number = ${ passportNum },
								calibration_interval = ${ interval } where guid = '${ guid }' returning *`
			
				let queryLog = `insert into meter_log (meter_guid, meter_serial_number, oper_type, issuing_person,
								accepted_person, datetime, update_field, comment_field)
                                values (
                                '${ guid }',
                                '${ serialNumber }',
                                 8,
                                 0,
                                 ${ editorStaffId },
                                 '${ getCurrentDateTime() }',
                                 '${ updateField }',
                                 '${ commentLog }')`
				
				const { rows } = await client.query(query)
				const [ editedMeter ] = rows
				await client.query(queryLog)
				apiRes.send(editedMeter)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(`ошибка при редактировании ${ e.message || e.detail }`)
			} finally {
				client.release()
			}
		})
		
		app.post(`/api/${ module_name }/create-accept-or-issue-log`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateCreateLog(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const {
				meters,
				operationType,
				newLocation,
				issuingPersonStaffId,
				acceptedPersonStaffId,
				comment
			} = apiReq.body
			
			console.log(operationType, newLocation, issuingPersonStaffId, acceptedPersonStaffId, comment)
			
			const client = await pgPool.connect()
			const results = []
			try {
				for (const meter of meters) {
					const query = `update meter set meter_location = ${ newLocation },
					               current_owner = ${ acceptedPersonStaffId }
					               where guid = '${ meter.guid }' returning *`
					const { oldLocation } = meters[0]
					const queryLog = "insert into meter_log (meter_guid,meter_serial_number, oper_type, issuing_person, " +
						"accepted_person, datetime, comment_field, old_location, new_location)  " +
						`values (
				                        '${ meter.guid }',
				                        '${ meter.serialNumber }',
				                         ${ operationType },
				                         ${ issuingPersonStaffId },
				                         ${ acceptedPersonStaffId },
				                         '${ getCurrentDateTime() }',
				                         '${ comment }',
				                         ${ meters[0].oldLocation },
				                         ${ newLocation }
				                    )`
					
					const { rows } = await client.query(query)
					const [ movedMeter ] = rows
					await client.query(queryLog)
					results.push({ ...movedMeter, success: true })
				}
				
				apiRes.send(results)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(`ошибка при приеме/выдаче ${ e.message || e.detail }`)
			} finally {
				client.release()
			}
		})
		
		app.post(`/api/${ module_name }/delete`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateDeleteMeter(apiReq.body)
			if (error)
				return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { guid, editorStaffId, meter } = apiReq.body
			const client = await pgPool.connect()
			
			try {
				const queryLog = "insert into meter_deleted (meter_info, meter_guid, person, date_time) values (" +
								`'${ JSON.stringify(meter) }',
				                 '${ guid }',
			                      ${ editorStaffId },
				                 '${ getCurrentDateTime() }')`
				
				const { rows } = await client.query(`delete from meter where guid = '${ guid }' returning guid`)
				const [ deletedMeter ] = rows
				await client.query(queryLog)
				apiRes.send(deletedMeter.guid)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(`ошибка при удалении ${ e.message || e.detail }`)
			} finally {
				client.release()
			}
		})
	}
}

function _validateDeleteMeter(meter) {
	const schema = {
		guid: joi.string().required(),
		editorStaffId: joi.number().required(),
		meter: joi.object().required(),
	}
	return joi.validate(meter, schema)
}

function _validateCheckMeter(meter) {
	const schema = {
		serialNumber: joi.string().required(),
		type: joi.number().required()
	}
	return joi.validate(meter, schema)
}

function _validateCreateLog(requestBody) {
	const schema = {
		meters: joi.array().min(1).required(),
		operationType: joi.number().required(),
		newLocation: joi.number().required(),
		issuingPersonStaffId: joi.number().required(),
		acceptedPersonStaffId: joi.number().required(),
		comment: joi.string().empty('')
	}
	return joi.validate(requestBody, schema)
}

function _validateRegisterMeter(meter) {
	const schema = {
		meters: joi.array().min(1).required(),
		accuracyClass: joi.number().required(),
		condition: joi.number().required(),
		interval: joi.number().required(),
		owner: joi.number().required(),
		issuingPersonStaffId: joi.number().required(),
		acceptedPersonStaffId: joi.number().required(),
		calibration: joi.string().allow(null).required(),
		comment: joi.string().empty(''),
		passportNumber: joi.number(),
		isRouter: joi.boolean(),
		storageType: joi.number().required(),
	}
	return joi.validate(meter, schema);
}

function _validateEditMeter(meter) {
	const schema = {
		type: joi.number().required(),
		serialNumber: joi.string().required(),
		accuracyClass: joi.number().required(),
		passportNumber: joi.number().allow(null),
		condition: joi.number().required(),
		interval: joi.number().required(),
		owner: joi.number().required(),
		editorStaffId: joi.number().required(),
		calibration: joi.string().allow(null).required(),
		comment: joi.string().empty(''),
		updateField: joi.string().required(),
		guid: joi.string().required()
	}
	return joi.validate(meter, schema);
}

function s4() {
	return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
}

function generateGuid() {
	return `${ s4() }${ s4() }-${ s4() }-${ s4() }-${ s4() }-${ s4() }${ s4() }${ s4() }`
}