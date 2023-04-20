const { getOraConnectionUit } = require("../../database/oracle/oracle-db-connection"),
{ joi, executeOraQuery } = require('../../utils'),
{ checkAuth } = require('../../login/login-api')

module.exports = class ReportsStorageApi {
	constructor(app, module_name) {
		
		app.get(`/api/${ module_name }/get-location-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra = 'SELECT meter_location, count(meter_location) AS count FROM meter GROUP BY meter_location'
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.get(`/api/${ module_name }/get-owner-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra = 'SELECT current_owner, count(meter_location) AS count FROM meter GROUP BY current_owner'
				
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-meter-report`, async (apiReq, apiRes) => {
			const { error } = validateMeterReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { type, serialNumber } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra =
					"select to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, ml.oper_type, " +
					" ml.issuing_person, ml.accepted_person, ml.comment_field," +
					" ml.update_field from meter_log ml, meter m where " +
					`ml.meter_guid = m.guid and m.serial_number = '${ serialNumber }' and ` +
					`m.meter_type = ${ type } order by ml.id`
				
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-storage-logs-by-period-report`, async (apiReq, apiRes) => {
			const { error } = validateStoragePeriodReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra =
					"select to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, ml.oper_type, " +
					"m.serial_number, ml.issuing_person, ml.accepted_person, ml.comment_field " +
					"from meter_log ml, meter m " +
					`where datetime between TO_DATE('${ startDate } 00:00', 'yyyy-mm-dd hh24:mi') ` +
					`and TO_DATE('${ endDate } 23:59', 'yyyy-mm-dd hh24:mi') and m.guid = ml.meter_guid order by datetime`
				
				console.log(queryOra)
				
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-in-out-by-period-and-location-report`, async (apiReq, apiRes) => {
			const { error } = validateLocationReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { location, startDate, endDate } = apiReq.body
			const operationType = location === 0 ? '0, 7, 9' : location
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOraTypesAndCountStart = getQueryStartOrEndCount({ startDate }, { operationType })
				const queryOraTypesAndEnd = getQueryStartOrEndCount({ endDate }, { operationType })
				const queryComingCount = getQueryComingCount({ location }, startDate, endDate)
				const queryLeaveCount = getQueryLeaveCount({ location }, startDate, endDate)
				
				let typesAndCountStart = await oraConn.execute(queryOraTypesAndCountStart)
				let typesAndCountEnd = await oraConn.execute(queryOraTypesAndEnd)
				let comingCount = await oraConn.execute(queryComingCount)
				let leaveCount = await oraConn.execute(queryLeaveCount)
				
				oraConn.close()
				
				const meterCountMap = new Map()
				
				fillMeterCountMap(meterCountMap, 'startDateCount', typesAndCountStart.rows)
				fillMeterCountMap(meterCountMap, 'comingCount', comingCount.rows)
				fillMeterCountMap(meterCountMap, 'leaveCount', leaveCount.rows)
				fillMeterCountMap(meterCountMap, 'endDateCount', typesAndCountEnd.rows)
				
				apiRes.status(200).send(Object.fromEntries(meterCountMap))
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-in-out-by-period-and-emp-report`, async (apiReq, apiRes) => {
			const { error } = validateEmpReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { empStaffId, startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOraTypesAndCountStart = getQueryStartOrEndCount({ startDate }, { empStaffId })
				const queryOraTypesAndEnd = getQueryStartOrEndCount({ endDate }, { empStaffId })
				const queryComingCount = getQueryComingCount({ empStaffId }, startDate, endDate)
				const queryLeaveCount = getQueryLeaveCount({ empStaffId }, startDate, endDate)
				
				let typesAndCountStart = await oraConn.execute(queryOraTypesAndCountStart)
				let typesAndCountEnd = await oraConn.execute(queryOraTypesAndEnd)
				let comingCount = await oraConn.execute(queryComingCount)
				let leaveCount = await oraConn.execute(queryLeaveCount)
				
				oraConn.close()
				
				const meterCountMap = new Map()
				
				fillMeterCountMap(meterCountMap, 'startDateCount', typesAndCountStart.rows)
				fillMeterCountMap(meterCountMap, 'comingCount', comingCount.rows)
				fillMeterCountMap(meterCountMap, 'leaveCount', leaveCount.rows)
				fillMeterCountMap(meterCountMap, 'endDateCount', typesAndCountEnd.rows)
				
				apiRes.status(200).send(Object.fromEntries(meterCountMap))
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-storage-logs-by-location-report`, async (apiReq, apiRes) => {
			const { error } = validateLocationReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { location, startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra = getQueryLogsBy({ location }, startDate, endDate)
				
				const { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-storage-logs-by-emp-report`, async (apiReq, apiRes) => {
			const { error } = validateEmpReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { empStaffId, startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra = getQueryLogsBy({ empStaffId }, startDate, endDate)
				
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				apiRes.status(200).send(rows)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-storage-group-logs-by-emp-report`, async (apiReq, apiRes) => {
			const { error } = validateEmpReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { empStaffId, startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra =
					"select meter.meter_type as type, meter_log.oper_type, " +
					"to_char(meter_log.datetime, 'dd.mm.yyyy') as datetime, " +
					"meter_log.issuing_person, meter_log.accepted_person " +
					"from meter, meter_log " +
					"where meter_log.meter_guid = meter.guid " +
					`and meter_log.datetime between TO_DATE('${ startDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
					`and TO_DATE('${ endDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
					`and (meter_log.issuing_person = ${ empStaffId } or meter_log.accepted_person = ${ empStaffId }) ` +
					"order by meter.meter_type, meter.serial_number, meter_log.datetime"
				
				let { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				const data = rows.reduce((acc, row) => {
					const findedRow = acc.find(
						groupRow =>
							groupRow.date === row.DATETIME &&
							groupRow.acceptedPerson === row.ACCEPTED_PERSON &&
							groupRow.issuingPerson === row.ISSUING_PERSON &&
							groupRow.type === row.TYPE &&
							groupRow.operationType === row.OPER_TYPE
					)
					
					if (!findedRow) {
						acc.push({
							date: row.DATETIME,
							type: row.TYPE,
							count: 1,
							operationType: row.OPER_TYPE,
							issuingPerson: row.ISSUING_PERSON,
							acceptedPerson: row.ACCEPTED_PERSON
						})
					} else {
						findedRow.count += 1
					}
					
					return acc
				}, [])
				
				apiRes.status(200).send(data)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
		
		app.post(`/api/${ module_name }/get-storage-count-by-current-location-report`, async (apiReq, apiRes) => {
			const { error } = validateLocationReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { location, startDate, endDate } = apiReq.body
			const oraConn = await getOraConnectionUit()
			
			try {
				const queryOra =
					"select m.meter_type, m.serial_number, " +
					"to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, " +
					"ml.issuing_person, ml.accepted_person, ml.comment_field from meter m, meter_log ml " +
					`where m.meter_location = ${ location } and m.guid = ml.meter_guid ` +
					`and ml.datetime between TO_DATE('${ startDate } 00:00', 'dd.mm.yyyy hh24:mi:ss') ` +
					`and TO_DATE('${ endDate } 23:59', 'dd.mm.yyyy hh24:mi:ss') order by datetime`
				
				const { rows } = await oraConn.execute(queryOra)
				oraConn.close()
				
				const meterMap = new Map()
				rows.forEach((row) => {
					if (!meterMap.get(row.SERIAL_NUMBER))
						meterMap.set(row.SERIAL_NUMBER, [ row ])
					else {
						const logs = meterMap.get(row.SERIAL_NUMBER)
						logs.push(row)
						meterMap.set(row.SERIAL_NUMBER, logs)
					}
				})
				
				const data = []
				for (const entry of meterMap.entries()) {
					let logs = entry[1].sort((a, b) => (new Date(b.DATETIME)).getTime() - (new Date(a.DATETIME)).getTime())
					let serialNumber = entry[0]
					let firstLog = logs[0]
					data.push({
						type: firstLog.METER_TYPE,
						serialNumber,
						date: firstLog.DATETIME,
						issuingPerson: firstLog.ISSUING_PERSON,
						acceptedPerson: firstLog.ACCEPTED_PERSON,
						comment: firstLog.COMMENT_FIELD
					})
				}
				
				apiRes.status(200).send(data)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			}
		})
	}
}

function getQueryLogsBy({ location, empStaffId }, startDate, endDate) {
	let sqlPart = ''
	if (location || location === 0) {
		sqlPart = `and (meter_log.new_location = ${ location } or meter_log.old_location = ${ location }) `
	} else if (empStaffId) {
		sqlPart = `and (meter_log.issuing_person = ${ empStaffId } or meter_log.accepted_person = ${ empStaffId }) `
	}
	
	return "select meter.meter_type as type, meter.serial_number as serNumber, " +
		"to_char(meter_log.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, " +
		"meter_log.oper_type, " +
		"meter_log.issuing_person, meter_log.accepted_person, meter_log.comment_field  " +
		"from meter, meter_log " +
		"where meter_log.meter_guid = meter.guid " +
		`and meter_log.datetime between TO_DATE('${ startDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		`and TO_DATE('${ endDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		`${ sqlPart } ` +
		"order by meter.meter_type, meter.serial_number, meter_log.datetime"
}

function getQueryStartOrEndCount({ startDate, endDate }, { empStaffId, operationType }) {
	let sqlPart = ''
	if (operationType) {
		sqlPart = `and meter_log.oper_type in (${ operationType }) `
	} else if (empStaffId) {
		sqlPart = `and meter_log.accepted_person = ${ empStaffId } `
	}
	
	return "select meter.meter_type as type, count(meter.meter_type) as count " +
		"from " +
		"(select max(meter_log.datetime) as lastlogtime, meter.guid as guid " +
		"from meter, meter_log " +
		"where meter_log.meter_guid = meter.guid " +
		`and meter_log.datetime < TO_DATE('${ startDate ? startDate : endDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		"group by meter.guid) a, meter_log, meter " +
		"where meter_log.datetime = a.lastlogtime " +
		"and meter_log.meter_guid = a.guid " +
		`${ sqlPart } ` +
		"and meter.guid = meter_log.meter_guid " +
		"group by meter.meter_type " +
		"order by meter.meter_type"
}

function getQueryComingCount({ location, empStaffId }, startDate, endDate) {
	let sqlPart = ''
	let sqlPart2 = ''
	if (location || location === 0) {
		sqlPart = `where meter_log.new_location in (${ location }) `
	} else if (empStaffId) {
		sqlPart = `where meter_log.accepted_person = ${ empStaffId } `
		sqlPart2 = "and meter_log.oper_type != 8 "
	}
	
	return "select meter.meter_type as type, count(meter.meter_type) as count " +
		"from meter, meter_log " +
		`${sqlPart} ` +
		"and meter.guid = meter_log.meter_guid " +
		`${ sqlPart2 } ` +
		`and meter_log.datetime between TO_DATE('${ startDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		`and TO_DATE('${ endDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		"group by meter.meter_type"
}

function getQueryLeaveCount({ location, empStaffId }, startDate, endDate) {
	let sqlPart = ''
	let sqlPart2 = ''
	if (location || location === 0) {
		sqlPart = `where meter_log.old_location in (${ location }) `
	} else if (empStaffId) {
		sqlPart = `where meter_log.issuing_person = ${ empStaffId } `
		sqlPart2 = "and meter_log.oper_type != 7 "
	}
	
	return "select meter.meter_type as type, count(meter.meter_type) as count " +
		"from meter, meter_log " +
		`${ sqlPart } ` +
		"and meter.guid = meter_log.meter_guid " +
		`${ sqlPart2 } ` +
		`and meter_log.datetime between TO_DATE('${ startDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		`and TO_DATE('${ endDate }', 'dd.mm.yyyy hh24:mi:ss') ` +
		"group by meter.meter_type"
}

function fillMeterCountMap(map, field, queryRows) {
	for (const row of queryRows) {
		if (map.has(row.TYPE)) {
			map.get(row.TYPE)[ field ] = row.COUNT
		} else {
			const count = {}
			count[ field ] = row.COUNT
			map.set(row.TYPE, count)
		}
	}
}

function validateMeterReport(report) {
	const schema = {
		type: joi.number().required(),
		serialNumber: joi.string().required(),
	}
	return joi.validate(report, schema)
}

function validateStoragePeriodReport(report) {
	const schema = {
		startDate: joi.string().required(),
		endDate: joi.string().required(),
	}
	return joi.validate(report, schema)
}

function validateLocationReport(report) {
	const schema = {
		location: joi.number().required(),
		startDate: joi.string().required(),
		endDate: joi.string().required(),
	}
	return joi.validate(report, schema)
}

function validateEmpReport(report) {
	const schema = {
		empStaffId: joi.number().required(),
		startDate: joi.string().required(),
		endDate: joi.string().required(),
	}
	return joi.validate(report, schema)
}

