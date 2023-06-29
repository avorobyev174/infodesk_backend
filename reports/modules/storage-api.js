const { pgPool } = require("../../database/postgres/postgres-db-connection"),
{ joi, executePGIQuery } = require('../../utils'),
{ checkAuth } = require('../../login/login-api')

module.exports = class ReportsStorageApi {
	constructor(app, module_name) {
		
		app.get(`/api/${ module_name }/get-location-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			executePGIQuery('SELECT meter_location, count(meter_location) ' +
									'AS count FROM meter GROUP BY meter_location', apiRes)
			
		})
		
		app.get(`/api/${ module_name }/get-owner-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			executePGIQuery('SELECT current_owner, count(meter_location) AS count' +
				' FROM meter GROUP BY current_owner', apiRes)
			
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
			const query =
				"select to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, ml.oper_type, " +
				" ml.issuing_person, ml.accepted_person, ml.comment_field," +
				" ml.update_field from meter_log ml, meter m where " +
				`ml.meter_guid = m.guid and m.serial_number = '${ serialNumber }' and ` +
				`m.meter_type = ${ type } order by ml.id`
			
			executePGIQuery(query, apiRes)
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
			const query =
				"select to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, ml.oper_type, " +
				"m.meter_type as type, m.serial_number, ml.issuing_person, ml.accepted_person, ml.comment_field " +
				"from meter_log ml, meter m " +
				`where datetime between '${ startDate } 00:00:00' ` +
				`and '${ endDate } 23:59:00' and m.guid = ml.meter_guid order by datetime`
			
			executePGIQuery(query, apiRes)
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
			const client = await pgPool.connect()
			
			try {
				const queryTypesAndCountStart = getQueryStartOrEndCount({ startDate }, { operationType })
				const queryTypesAndEnd = getQueryStartOrEndCount({ endDate }, { operationType })
				const queryComingCount = getQueryComingCount({ location }, startDate, endDate)
				const queryLeaveCount = getQueryLeaveCount({ location }, startDate, endDate)
				
				let typesAndCountStart = await client.query(queryTypesAndCountStart)
				let typesAndCountEnd = await client.query(queryTypesAndEnd)
				let comingCount = await client.query(queryComingCount)
				let leaveCount = await client.query(queryLeaveCount)
				
				const meterCountMap = new Map()
				
				fillMeterCountMap(meterCountMap, 'startDateCount', typesAndCountStart.rows)
				fillMeterCountMap(meterCountMap, 'comingCount', comingCount.rows)
				fillMeterCountMap(meterCountMap, 'leaveCount', leaveCount.rows)
				fillMeterCountMap(meterCountMap, 'endDateCount', typesAndCountEnd.rows)
				
				apiRes.status(200).send(Object.fromEntries(meterCountMap))
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
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
			const client = await pgPool.connect()
			
			try {
				const queryOraTypesAndCountStart = getQueryStartOrEndCount({ startDate }, { empStaffId })
				const queryOraTypesAndEnd = getQueryStartOrEndCount({ endDate }, { empStaffId })
				const queryComingCount = getQueryComingCount({ empStaffId }, startDate, endDate)
				const queryLeaveCount = getQueryLeaveCount({ empStaffId }, startDate, endDate)
				
				let typesAndCountStart = await client.query(queryOraTypesAndCountStart)
				let typesAndCountEnd = await client.query(queryOraTypesAndEnd)
				let comingCount = await client.query(queryComingCount)
				let leaveCount = await client.query(queryLeaveCount)
				
				const meterCountMap = new Map()
				
				fillMeterCountMap(meterCountMap, 'startDateCount', typesAndCountStart.rows)
				fillMeterCountMap(meterCountMap, 'comingCount', comingCount.rows)
				fillMeterCountMap(meterCountMap, 'leaveCount', leaveCount.rows)
				fillMeterCountMap(meterCountMap, 'endDateCount', typesAndCountEnd.rows)
				
				apiRes.status(200).send(Object.fromEntries(meterCountMap))
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
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
			const query = getQueryLogsBy({ location }, startDate, endDate)
			executePGIQuery(query, apiRes)
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
			const query = getQueryLogsBy({ empStaffId }, startDate, endDate)
			executePGIQuery(query, apiRes)
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
			const client = await pgPool.connect()
			
			try {
				const query =
					"select meter.meter_type as type, meter_log.oper_type, " +
					"to_char(meter_log.datetime, 'dd.mm.yyyy') as datetime, " +
					"meter_log.issuing_person, meter_log.accepted_person " +
					"from meter, meter_log " +
					"where meter_log.meter_guid = meter.guid " +
					`and meter_log.datetime between '${ startDate }' ` +
					`and '${ endDate }' ` +
					`and (meter_log.issuing_person = ${ empStaffId } or meter_log.accepted_person = ${ empStaffId }) ` +
					"order by meter.meter_type, meter.serial_number, meter_log.datetime"
				
				let { rows } = await client.query(query)
				
				const data = rows.reduce((acc, row) => {
					const foundRow = acc.find(
						groupRow =>
							groupRow.date === row.datetime &&
							groupRow.acceptedPerson === row.accepted_person &&
							groupRow.issuingPerson === row.issuing_person &&
							groupRow.type === row.type &&
							groupRow.operationType === row.oper_type
					)
					
					if (!foundRow) {
						acc.push({
							date: row.datetime,
							type: row.type,
							count: 1,
							operationType: row.oper_type,
							issuingPerson: row.issuing_person,
							acceptedPerson: row.accepted_person
						})
					} else {
						foundRow.count += 1
					}
					
					return acc
				}, [])
				
				apiRes.status(200).send(data)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
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
			const client = await pgPool.connect()
			
			try {
				const query =
					"select m.meter_type, m.serial_number, " +
					"to_char(ml.datetime, 'dd.mm.yyyy hh24:mi:ss') as datetime, " +
					"ml.issuing_person, ml.accepted_person, ml.comment_field from meter m, meter_log ml " +
					`where m.meter_location = ${ location } and m.guid = ml.meter_guid ` +
					`and ml.datetime between '${ startDate } 00:00:00' ` +
					`and '${ endDate } 23:59:00' order by datetime`
				
				const { rows } = await client.query(query)
				
				const meterMap = new Map()
				rows.forEach((row) => {
					if (!meterMap.get(row.serial_number))
						meterMap.set(row.serial_number, [ row ])
					else {
						const logs = meterMap.get(row.serial_number)
						logs.push(row)
						meterMap.set(row.serial_number, logs)
					}
				})
				
				const data = []
				for (const entry of meterMap.entries()) {
					let logs = entry[1].sort((a, b) => (new Date(b.datetime)).getTime() - (new Date(a.datetime)).getTime())
					let serialNumber = entry[0]
					let firstLog = logs[0]
					data.push({
						type: firstLog.meter_type,
						serialNumber,
						date: firstLog.datetime,
						issuingPerson: firstLog.issuing_person,
						acceptedPerson: firstLog.accepted_person,
						comment: firstLog.comment_field
					})
				}
				
				apiRes.status(200).send(data)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
			}
		})
		
		app.get(`/api/${ module_name }/get-repair-count-and-material-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const months = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
														"Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь" ]
			
			
			const today = new Date()
			const currentYear = today.getFullYear()
			const meterTypeMap = new Map()
			const client = await pgPool.connect()
			
			try {
				for (const month of months) {
					let monthNumber = months.indexOf(month) + 1
					let date = new Date(currentYear, monthNumber, 0)
					let startDate = `${ currentYear }-${ monthNumber }-1 00:00`
					let endDate = `${ currentYear }-${ monthNumber }-${ date.getDate() } 23:59`
				
					const query =
						"select x.type, count(x.statusOK) as repaired, count(x.statusNotOk) as broken " +
							"from (select meter.meter_type as type, st1.status as statusOK, st2.status as statusNotOk " +
							"from meter right join meter_log on meter.guid = meter_log.meter_guid " +
							"full outer join meter_work_status st1 on st1.log_id = meter_log.id and st1.status = 1 " +
							`and st1.datetime between '${ startDate }' and '${ endDate }' ` +
							"full outer join meter_work_status st2 on st2.log_id = meter_log.id and st2.status = 0 " +
							`and st2.datetime between '${ startDate }' and '${ endDate }' ` +
							"where meter_log.oper_type = 1) as x group by type order by type"
					
					const { rows } = await client.query(query)
					
					for (const row of rows) {
						let finalMonthMapArray = []
						const meterMonthMap = new Map()
						
						if (row.repaired || row.broken ) {
							meterMonthMap.set(monthNumber, { repaired : row.repaired, broken : row.broken })
						}
				
						if (meterTypeMap.has(row.type)) {
							const monthMapArray = meterTypeMap.get(row.type)
							monthMapArray.push(meterMonthMap)
							finalMonthMapArray = monthMapArray
						} else {
							if (meterMonthMap.size) {
								finalMonthMapArray.push(meterMonthMap)
							}
						}
						
						if (finalMonthMapArray.length) {
							meterTypeMap.set(row.type, finalMonthMapArray)
						}
					}
				}
				
				
				let data = []
				let records = []
				
				for (let entry of meterTypeMap) {
					const [ meterType, monthMapArray ] = entry
					if (!meterType) {
						continue
					}
					
					let record = []
					let row = []
					row.push(meterType)
					
					for (let i = 1; i <= today.getMonth() + 1; i++) {
						let infoFind = false
						for (let j = 0; j < monthMapArray.length; j++) {
							if (monthMapArray[j].has(i)) {
								infoFind = true
								record.push(parseInt(monthMapArray[j].get(i).repaired))
								record.push(parseInt(monthMapArray[j].get(i).broken))
								row.push([
									parseInt(monthMapArray[j].get(i).repaired),
									parseInt(monthMapArray[j].get(i).broken)
								])
							}
						}
						if (!infoFind) {
							record.push(0)
							record.push(0)
							row.push([ 0, 0 ])
						}
					}
					
					let sumRepaired = 0
					let sumBroken = 0
					for (let i = 0; i < record.length; i++) {
						i % 2 === 0 ? sumRepaired += record[i] : sumBroken += record[i]
					}
					
					row.push([ sumRepaired, sumBroken ])
					
					const sum = sumBroken/(sumBroken + sumRepaired) * 100
					row.push(isNaN(sum) || sum === 0 ? '0%' : sum.toFixed(2) + '%')
					
					if (!sumRepaired && !sumBroken) {
						continue
					}
					records.push(record)
					data.push(row)
				}
				
				const row = []
				let totalRow = []
				
				for (let j = 0; j < (today.getMonth() + 1) * 2; j++) {
					let total = 0
					for (let i = 0; i < records.length; i++) {
						total += records[i][j]
					}
					totalRow.push(total)
				}
				
				for (let j = 0; j < totalRow.length; j += 2) {
					row.push([ totalRow[j] , totalRow[j + 1] ])
				}
				
				let sumOk = 0, sumBreak = 0
				for (let i = 0; i < totalRow.length; i++) {
					i % 2 === 0 ? sumOk += totalRow[i] : sumBreak += totalRow[i]
				}
				row.push([ sumOk, sumBreak ])
				
				const sum = sumBreak/(sumBreak + sumOk) * 100
				row.push(isNaN(sum) || sum === 0 ? '0%' : sum.toFixed(2) + '%')
				data.push(row)
				
				apiRes.status(200).send(data)
				
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
			}
		})
		
		app.get(`/api/${ module_name }/get-spent-by-year-report`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			const materialMap = new Map()
			const client = await pgPool.connect()
			
			try {
				const yearSpentQuery =
					"select item_id, sum(amount) as amount " +
					"from meter_spent_item " +
					"where item_id in (1, 2, 3, 4, 5, 6, 13, 14) " +
					`and datetime >= '01.01.${ (new Date()).getFullYear() }  00:01'  ` +
					"group by item_id order by item_id"
				
				const yearMaterialSpent = await client.query(yearSpentQuery)
				for (const row of yearMaterialSpent.rows) {
					materialMap.set(row.item_id, { spentYearAmount : row.amount })
				}
			
				const totalSpentQuery =
					"select item_id, sum(amount) as amount " +
					"from meter_spent_item " +
					"where item_id in (1, 2, 3, 4, 5, 6, 13, 14) " +
					"group by item_id order by item_id"
				
				const totalMaterialSpent = await client.query(totalSpentQuery)
				for (const row of totalMaterialSpent.rows) {
					if (materialMap.has(row.item_id)) {
						materialMap.get(row.item_id).spentAmount = row.amount
					} else {
						materialMap.set(row.item_id, { spentAmount: row.amount })
					}
				}
				
				const storageQuery =
					"select item_id, sum(amount) as amount " +
					"from meter_item_storage " +
					"where item_id in (1, 2, 3, 4, 5, 6, 13, 14) " +
					"group by item_id order by item_id"
				
				const storageMaterial = await client.query(storageQuery)
				for (const row of storageMaterial.rows) {
					if (materialMap.has(row.item_id)) {
						materialMap.get(row.item_id).storageAmount = row.amount
					} else {
						materialMap.set(row.item_id, { storageAmount: row.amount })
					}
				}
				
				const data = []
				for (const entry of materialMap) {
					const [itemId, amount ] = entry
					let { spentAmount, storageAmount, spentYearAmount } = amount
					spentAmount = spentAmount || 0
					storageAmount = storageAmount || 0
					spentYearAmount = spentYearAmount || 0
					const totalAmount = storageAmount - spentAmount
					
					data.push([
						itemId,
						spentYearAmount,
						totalAmount
					])
				}
				
				apiRes.status(200).send(data)
			} catch ({ message }) {
				return apiRes.status(400).send(message)
			} finally {
				client.release()
			}
		})
		
		app.post(`/api/${ module_name }/get-spent-by-month-report`, async (apiReq, apiRes) => {
			const { error } = validateStoragePeriodReport(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { startDate, endDate } = apiReq.body
			const query =
				"select item_id, sum(amount) as amount " +
				"from meter_spent_item " +
				"where item_id in (1, 2, 3, 4, 5, 6, 13, 14) " +
				`and datetime between '${ startDate } 00:00:01' ` +
				`and '${ endDate } 23:59:00' group by item_id order by item_id`
			
			executePGIQuery(query, apiRes)
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
		`and meter_log.datetime between '${ startDate }' ` +
		`and '${ endDate }' ` +
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
		`and meter_log.datetime < '${ startDate ? startDate : endDate }' ` +
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
		`and meter_log.datetime between '${ startDate }' ` +
		`and '${ endDate }' ` +
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
		`and meter_log.datetime between '${ startDate }' ` +
		`and '${ endDate }' ` +
		"group by meter.meter_type"
}

function fillMeterCountMap(map, field, queryRows) {
	for (const row of queryRows) {
		if (map.has(row.type)) {
			map.get(row.type)[ field ] = row.count
		} else {
			const count = {}
			count[ field ] = row.count
			map.set(row.type, count)
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

