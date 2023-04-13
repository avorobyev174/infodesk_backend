const { showRequestInfoAndTime, joi, getDateTime, executeOraQuery  } = require('../../utils'),
{ getOraConnectionUit } = require("../../database/oracle/oracle-db-connection"),
{ checkAuth } = require('../../login/login-api'),
oracledb = require('oracledb')

module.exports = class acceptOrIssueApi {
	constructor(app, module_name) {
		app.post(`/api/${ module_name }/check-meter`, (apiReq, apiRes) => {
			
			const { error } = _validateCheckMeter(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes))
				return
				
			const { serialNumber, type } = apiReq.body
			
			showRequestInfoAndTime('Склад счетчиков: запрос на получение счетчика по type и serialNumber')
			
			const query = `select * from meter where meter_type = ${ type } and serial_number = '${ serialNumber }'`
			
			executeOraQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/registration`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateRegisterMeter(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes))
				return
			
			const {
				meters,
				acceptedPersonStaffId,
				accuracyClass,
				condition,
				issuingPersonStaffId,
				interval,
				owner,
				calibration,
				passportNumber,
				comment,
				isRouter
			} = apiReq.body
			
			const calibrationDate = !calibration ? null : `TO_DATE('${ calibration }', 'yyyy-mm-dd')`
			const operationType = isRouter ? 1 : 7
			const meterLocation = isRouter ? 1 : 0
			showRequestInfoAndTime('Склад счетчиков: запрос на регистрация счетчика')
			
			const oraConn = await getOraConnectionUit()
			
			const meterQuerySet = meters.map(meter => {
				const guid = generateGuid()
				let query = `insert into meter (meter_type, serial_number, guid, accuracy_class, condition, passport_number,
				                calibration_date, calibration_interval, meter_location, current_owner, property, lv_modem)
								values (
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
                                0) returning id into :meter_id`
				
				let queryLog = `insert into meter_log (meter_guid, meter_serial_number, oper_type, issuing_person,
									accepted_person, datetime, old_location, new_location, comment_field)
                                    values (
	                                '${ guid }',
	                                '${ meter.serialNumber }',
	                                 ${ operationType },
	                                 ${ issuingPersonStaffId },
	                                 ${ acceptedPersonStaffId },
	                                 TO_DATE('${ getDateTime() }', 'yyyy-mm-dd hh24:mi:ss'),
	                                 null,
	                                 ${ meterLocation },
	                                 '${ comment }')`
				console.log(query)
				console.log(queryLog)
				
				const meterOut = {}
				meterOut.meter_id = { dir: oracledb.BIND_OUT, type: oracledb.NUMBER }
				
				const querySet = [ oraConn.execute(query, meterOut), oraConn.execute(queryLog) ]
				
				const createdMeterObj = {
					meter_type: meter.type,
					serial_number: meter.serialNumber,
					guid,
					accuracy_class: accuracyClass,
					condition,
					calibration_date: calibrationDate,
					calibration_interval: interval,
					meter_location: 0,
					current_owner: acceptedPersonStaffId,
					property: owner
				}
				return { createdMeterObj, querySet }
			})
			
			meterQuerySet.results = []
			
			for (const meterQuery of meterQuerySet) {
				await Promise
					.all(meterQuery.querySet)
					.then(
						results => {
							console.log(results)
							if (results.length === 2 && results[0].outBinds && results[0].outBinds.meter_id) {
								meterQuerySet.results.push({
									...meterQuery.createdMeterObj,
									id: results[0].outBinds.meter_id[0],
									success: true
								})
							} else {
								meterQuerySet.results.push({
									...meterQuery.createdMeterObj,
									success: false
								})
							}
						},
						error => {
							console.log(error)
							meterQuerySet.results.push({
								...meterQuery.createdMeterObj,
								success: false
							})
						}
					)
				
				console.log(meterQuerySet.results)
				//Только после последнего выполенного
				if (meterQuerySet.length === meterQuerySet.results.length) {
					apiRes.send(meterQuerySet.results)
					oraConn.close()
				}
			}
		})
		
		app.post(`/api/${ module_name }/edit`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateEditMeter(apiReq.body)
			if (error)
				return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes))
				return
			
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
			
			const calibrationDate = !calibration ? null : `TO_DATE('${ calibration }', 'yyyy-mm-dd')`
			const commentLog = comment ? comment : ''
			const passportNum = passportNumber ? passportNumber : 0
			showRequestInfoAndTime('Склад счетчиков: запрос на редактирование счетчика')
			
			const oraConn = await getOraConnectionUit()
			
			let query = `update meter set meter_type = ${ type }, serial_number = ${ serialNumber },
				accuracy_class = ${ accuracyClass }, condition = ${ condition },
				calibration_date = ${ calibrationDate }, passport_number = ${ passportNum },
				calibration_interval = ${ interval } where guid = '${ guid }'`
			
			let queryLog = `insert into meter_log (meter_guid, meter_serial_number, oper_type, issuing_person,
								accepted_person, datetime, update_field, comment_field)
                                values (
                                '${ guid }',
                                '${ serialNumber }',
                                 8,
                                 0,
                                 ${ editorStaffId },
                                 TO_DATE('${ getDateTime() }', 'yyyy-mm-dd hh24:mi:ss'),
                                 '${ updateField }',
                                 '${ commentLog }')`
			console.log(query)
			console.log(queryLog)
			const querySet = [
				oraConn.execute(query),
				oraConn.execute(queryLog)
			]

			const results = []

			await Promise
				.all(querySet)
				.then(
					result => {
						console.log(result)
						result.length === 2
							? results.push({ guid, success: true })
							: results.push({ guid, success: false })
					},
					error => {
						console.log(error)
						results.push({ guid, success: false })
					}
				)

			console.log(results)
			apiRes.send(results)
			oraConn.close()
		})
		
		app.post(`/api/${ module_name }/create-accept-or-issue-log`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateCreateLog(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes))
				return
			
			const { meters, operationType, newLocation, issuingPersonStaffId, acceptedPersonStaffId, comment } = apiReq.body
			
			console.log(operationType, newLocation, issuingPersonStaffId, acceptedPersonStaffId, comment)
			
			showRequestInfoAndTime('Склад счетчиков: запрос на создание лога')
			
			const oraConn = await getOraConnectionUit()
			
			const meterQuerySet = meters.map(meter => {
				const query = `update meter
			               set meter_location = ${ newLocation },
			               current_owner = ${ acceptedPersonStaffId }
			               where guid = '${ meter.guid }'`
				
				const queryLog = `insert into meter_log (meter_guid,meter_serial_number, oper_type,
					issuing_person,	accepted_person,datetime,comment_field,old_location,new_location)
                    values (
                        '${ meter.guid }',
                        '${ meter.serialNumber }',
                         ${ operationType },
                         ${ issuingPersonStaffId },
                         ${ acceptedPersonStaffId },
                         TO_DATE('${ getDateTime() }', 'yyyy-mm-dd hh24:mi:ss'),
                         '${ comment }',
                         ${ meters[0].oldLocation },
                         ${ newLocation }
                    )`
				
				const querySet = [
					oraConn.execute(query),
					oraConn.execute(queryLog)
				]
				return { guid: meter.guid, querySet }
			})
			
			meterQuerySet.results = []
			
			for (const meterQuery of meterQuerySet) {
				await Promise
					.all(meterQuery.querySet)
					.then(
						results => {
							console.log(results)
							results.length === 2
								? meterQuerySet.results.push({ guid: meterQuery.guid, success: true })
								: meterQuerySet.results.push({ guid: meterQuery.guid, success: false })
						},
						error => {
							console.log(error)
							meterQuerySet.results.push({ guid: meterQuery.guid, success: false })
						}
				)
				
				console.log(meterQuerySet.results)
				if (meterQuerySet.length === meterQuerySet.results.length) {
					apiRes.send(meterQuerySet.results)
					oraConn.close()
				}
			}
		})
		
		app.post(`/api/${ module_name }/delete`, async (apiReq, apiRes) => {
			console.log(apiReq.body)
			const { error } = _validateDeleteMeter(apiReq.body)
			if (error)
				return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes))
				return
			
			const { guid, editorStaffId, meter } = apiReq.body
			
			showRequestInfoAndTime('Склад счетчиков: запрос на удаление счетчика')
			
			const oraConn = await getOraConnectionUit()
			
			let query = `delete from meter where guid = '${ guid }'`
			
			let queryLog = `insert into meter_deleted (meter_info, meter_guid, person, date_time)
			 values ('${ JSON.stringify(meter) }', '${ guid }', ${ editorStaffId }, TO_DATE('${ getDateTime() }', 'yyyy-mm-dd hh24:mi:ss'))`
				
			console.log(query)
			console.log(queryLog)
			const querySet = [
				oraConn.execute(query),
				oraConn.execute(queryLog)
			]
			
			const results = []
			
			await Promise
				.all(querySet)
				.then(
					result => {
						console.log(result)
						result.length === 2
							? results.push({ guid, success: true })
							: results.push({ guid, success: false })
					},
					error => {
						console.log(error)
						results.push({ guid, success: false })
					}
				)

			console.log(results)
			apiRes.send(results)
			oraConn.close()
		})
	}
}

function _validateDeleteMeter(meter) {
	const schema = {
		guid: joi.string().required(),
		editorStaffId: joi.number().required(),
		meter: joi.object().required(),
	}
	return joi.validate(meter, schema);
}

function _validateCheckMeter(meter) {
	const schema = {
		serialNumber: joi.string().required(),
		type: joi.number().required()
	}
	return joi.validate(meter, schema);
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
	return joi.validate(requestBody, schema);
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
		calibration: joi.date().allow(null).required(),
		comment: joi.string().empty(''),
		passportNumber: joi.number(),
		isRouter: joi.boolean(),
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
		calibration: joi.date().allow(null).required(),
		comment: joi.string().empty(''),
		updateField: joi.string().required(),
		guid: joi.string().required()
	}
	return joi.validate(meter, schema);
}

function _s4() {
	return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
}
function generateGuid() {
	return `${_s4()}${_s4()}-${_s4()}-${_s4()}-${_s4()}-${_s4()}${_s4()}${_s4()}`
}