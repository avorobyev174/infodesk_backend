const { pgPool } = require("../database/postgres/postgres-db-connection")
const { pgStekASDPool } = require("../database/postgres/postgres-stek-asd-db-connection")
const { getCurrentDateTime, formatDateTime, showRequestInfoAndTime, joi, executePGIQuery, formatDateTimeForUser } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'meter-service'
const DAY_DEPTH = 3
const { AssignmentEventType, AssignmentStatus } = require("../const")
const axios = require('axios')
const { Agent } = require('https')
const { rtc } = require('../module-credentials')
const ASSIGNMENT_EVENT_CLOSE_REASON_NOW_AVAILABLE = 3
const CLOSE_REASON_TYPES_NEEDS_TO_CALLBACK = [ 2, 3, 4, 5, 7, 9, 10, 13, 14, 15, 16, 17 ]
const Schedule = require('node-schedule')
const UpdateScheduleRule = { days: [ 1, 2, 3, 4, 5 ], hour: 8, minute: 0 }

createScheduleRule()
//updateServiceSystem()

module.exports = class MeterServiceApi {
	constructor(app) {
		//Получение списка поручений
		app.get(`/api/${ module_name }/assignments`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			await executePGIQuery('select * from assignment', apiRes)
		})
		
		//Создание поручения
		app.post(`/api/${ module_name }/add-assignment`, async (apiReq, apiRes) => {
			const { error } = _validateAddAssignment(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { serialNumber, type } = apiReq.body
			const client = await pgPool.connect()
			const stekASDClient = await pgStekASDPool.connect()
			
			try {
				const assignment = await createAssignmentWithLastData(
					client,
					stekASDClient,
					apiRes,
					authResult,
					serialNumber,
					type
				)
				if (assignment) {
					const [ newAssignment ] = assignment
					const { id, last_data_date } = newAssignment
					await createAssignmentEvent(
						client,
						AssignmentEventType.REGISTERED,
						id,
						authResult.id,
						`Дата последнего опроса ${ last_data_date ? formatDateTimeForUser(last_data_date) : 'отсутствует' }`
					)
					apiRes.send(assignment)
				}
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
				stekASDClient.release()
			}
		})
		
		app.get(`/api/${ module_name }/assignments-logs`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			await executePGIQuery('select * from assignment_log order by created desc', apiRes)
		})
		
		//Редактирование поручения
		app.put(`/api/${ module_name }/assignment-accept/:id`, async (apiReq, apiRes) => {
			const { error } = _validateAssignment(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			const assignmentId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { customerContacts } = apiReq.body
			const client = await pgPool.connect()
			try {
				let query = ''
				if (customerContacts) {
					query = `update assignment
								set customer_contacts = '${ customerContacts }'
								where id = ${ assignmentId } returning *`
				} else {
					query = `update assignment
								set owner_id = ${ authResult.id },
								status = ${ AssignmentStatus.IN_WORK }
								where id = ${ assignmentId } returning *`
					
					await createAssignmentEvent(client, AssignmentEventType.IN_WORK, assignmentId, authResult.id)
				}
				
				const assignment = await executeQuery(client, query)
				apiRes.send(assignment)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		//Получение списка событий поручения
		app.get(`/api/${ module_name }/assignment-events/:id`, async (apiReq, apiRes) => {
			const assignmentId = apiReq.params.id
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			await executePGIQuery(`select * from assignment_event
									where assignment_id = ${ assignmentId } order by id desc`, apiRes)
		})
		
		//Создание события активности
		app.post(`/api/${ module_name }/add-action-assignment-event/:id`, async (apiReq, apiRes) => {
			const assignmentId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { description } = apiReq.body
			const client = await pgPool.connect()
			try {
				const assignmentEvent = await createAssignmentEvent(
					client,
					AssignmentEventType.ACTION,
					assignmentId,
					authResult.id,
					description
				)
				apiRes.send(assignmentEvent)
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		//Редактирование события активности
		app.put(`/api/${ module_name }/change-action-event/:id`, async (apiReq, apiRes) => {
			const actionEventId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { description } = apiReq.body
			const client = await pgPool.connect()
			try {
				const desc = description ? `'${ description }'` : null
				const assignmentEvent = await executeQuery(client,
					`update assignment_event set description = ${ desc }
							where id = ${ actionEventId } returning *`)
				
				apiRes.send(assignmentEvent)
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		//Удаление события активности
		app.post(`/api/${ module_name }/delete-action-assignment-event/:id`, async (apiReq, apiRes) => {
			const eventId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const client = await pgPool.connect()
			try {
				const assignmentEvent = await executeQuery(client,
					`delete from assignment_event where id = ${ eventId } returning *`)
				
				apiRes.send(assignmentEvent)
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		//Закрытие поручения
		app.post(`/api/${ module_name }/close-assignment/:id`, async (apiReq, apiRes) => {
			const { error } = _validateAssignment(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			const assignmentId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { closeReason, description } = apiReq.body
			const client = await pgPool.connect()
			try {
				const query = `update assignment
								 set status = ${ AssignmentStatus.CLOSED },
								 current_close_reason = ${ closeReason },
								 old_last_data_date = null
								 where id = ${ assignmentId } returning *`
				
				const assignmentEvent = await createAssignmentEvent(
					client,
					AssignmentEventType.CLOSED,
					assignmentId,
					authResult.id,
					description,
					closeReason
				)
				
				const assignment = await executeQuery(client, query)
				apiRes.send({ assignment: assignment[0], assignmentEvent: assignmentEvent[0] })
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		//Редактирование контактов
		app.post(`/api/${ module_name }/save-assignment-contacts/:id`, async (apiReq, apiRes) => {
			const assignmentId = apiReq.params.id
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			const { contacts } = apiReq.body
			const client = await pgPool.connect()
			try {
				const contact = contacts ? `'${ contacts }'` : null
				const assignment = await executeQuery(client,
					`update assignment set customer_contacts = ${ contact }
							where id = ${ assignmentId } returning *`)
				
				apiRes.send(assignment)
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
		
		
	}
}
function getDateTimePeriod() {
	const dateNow = new Date()
	const dayNow = dateNow.getDate()
	const monthNow = dateNow.getMonth() + 1
	const yearNow = dateNow.getFullYear()
	
	let dateFrom = new Date(yearNow, monthNow - 1, dayNow - DAY_DEPTH)
	dateFrom = `${ dateFrom.getFullYear() }-${ dateFrom.getMonth() + 1 }-${ dateFrom.getDate() }`
	const dateTo = `${ yearNow }-${ monthNow }-${ dayNow }`
	
	return { dateFrom, dateTo }
}

async function insertNewAssignments(client, nonActiveMetersWithLastData, assignments, simCardsUpdateData) {
	const newAssignments = []
	for (const meter of nonActiveMetersWithLastData) {
		const { last_date_time, serial_number } = meter
		const assignment = assignments.find(({ meter_serial_number }) => meter_serial_number === serial_number)
		
		if (!assignment) {
			const createdAssignment = await createAssignment(client, meter, null, simCardsUpdateData)
			
			// const contacts = `'${ customer_phone ? customer_phone : '' } ${ customer_email ? customer_email : '' }'`.trim()
			// const customerContacts = contacts ? contacts : null
			// const simCard = simCardsUpdateData.find(({ simPhone }) => simPhone === phone)
			// const finalSimStatus = simCard && simCard.simStatus !== status ? simCard.simStatus: status
			// const customerAddress = customer_address ? `'${ customer_address }'` : null
			// const personalAccount = personal_account ? `'${ personal_account }'` : null
			// const meterAddress = address ? `'${ address }'` : null
			// const meterPhone = phone ? `'${ phone }'` : null
			//
			// const createdAssignment = await executeQuery(client,
			// 	`insert into assignment (
			// 									meter_type,
			// 									meter_serial_number,
			// 									meter_ip_address,
			// 									meter_phone,
			// 									meter_sim_status,
			// 									customer_personal_account,
			// 									customer_address,
			// 									customer_contacts,
			// 									last_data_date,
			// 									created,
			// 									status,
			// 									meter_address,
			// 									meter_port,
			// 									meter_contact)
			// 								values (
			// 									${ type },
			// 									'${ serial_number }',
			// 									${ ip_address },
			// 									${ meterPhone },
			// 									${ finalSimStatus },
			// 									${ personalAccount },
			// 									${ customerAddress },
			// 									${ customerContacts },
			// 									${ lastDataDateTime },
			// 									'${ getCurrentDateTime() }',
			// 									${ AssignmentStatus.REGISTERED },
			// 									${ meterAddress },
			// 									${ port },
			// 									${ contact }
			// 								) returning *`)
			
			const [ newAssignment ] = createdAssignment
			const { id } = newAssignment
			await createAssignmentEvent(
				client,
				AssignmentEventType.REGISTERED,
				id,
				null,
				`Дата последнего опроса ${ last_date_time
					? formatDateTimeForUser(last_date_time)
					: 'отсутствует' }`
			)
			newAssignments.push({ assignment: newAssignment, status: 'new' })
		} else {
			const { id, current_close_reason } = assignment
			if (assignment.status === AssignmentStatus.CLOSED && CLOSE_REASON_TYPES_NEEDS_TO_CALLBACK.includes(current_close_reason)) {
				const lastDataDateTime = last_date_time ? `'${ last_date_time } '` : null
				const updatedAssignment = await executeQuery(client,
					`update assignment
							set last_data_date = ${ lastDataDateTime },
							status = ${ AssignmentStatus.RE_REGISTERED }
							where id = ${ id } returning *`)
				
				const [ reopenedAssignment ] = updatedAssignment
				await createAssignmentEvent(client, AssignmentEventType.RE_REGISTERED, id)
				newAssignments.push({ assignment: reopenedAssignment, status: 'reopen' })
			}
		}
	}
	
	return newAssignments
}

async function getEntityLastData(stekASDClient, entities) {
	const serialNumbers = entities.map(({ serial_number }) => `'${ serial_number }'`).join(',')
	const queryLastData = `select serial_number, max(to_char(date_time, 'YYYY-MM-DD HH24:MI:SS')) as last_date_time from (
								select ap.ЗавНомер as serial_number,
								data."ДатаВремя" as date_time,
								data."Значение" as value
								from stack."АСД Приборы" ap,
								stack."АСД Каналы" ac,
								stack."АСД ДанНаСут" data
								where ap.ЗавНомер in (${ serialNumbers })
								and ap.row_id = ac.Прибор
								and ac."ТипКанала" = 1000
								and ac.row_id = data.Канал
								order by data."ДатаВремя" desc) x group by serial_number`
	
	const { rows } = await stekASDClient.query(queryLastData)
	return entities.map((entity) => {
		const lastDataTime = rows.filter((row) => row.serial_number === entity.serial_number)
		if (lastDataTime.length) {
			const [ lastData ] = lastDataTime
			const { last_date_time } = lastData
			return { ...entity, last_date_time }
		}
		return entity
	})
}

async function getNonActiveMeters(client, stekASDClient) {
	const { dateFrom, dateTo } = getDateTimePeriod()
	const meters = await client.query('select * from meter_reg where in_pyramid = 1')
	const metersArray = meters.rows
	const strSerialNumbers = metersArray.map(({ serial_number }) => `'${ serial_number }'`).join(',')
	const query = `select max(x.date_time) as last_date_time,
								    x.serial_number,
								    max(x.value) as last_value from (
										select ap.ЗавНомер as serial_number,
									        data."ДатаВремя" as date_time,
											data."Значение" as value
											from stack."АСД Приборы" ap,
											stack."АСД Каналы" ac,
											stack."АСД ДанНаСут" data
											where ap.ЗавНомер in (${ strSerialNumbers })
											and ap.row_id = ac.Прибор
											and ac."ТипКанала" = 1000
											and ac.row_id = data.Канал
											and data."ДатаВремя" between '${ dateFrom }' and '${ dateTo }'
											) x
											group by x.serial_number`
	const responseASD = await stekASDClient.query(query)
	return metersArray
		.filter((meter) => !responseASD.rows
		.find((row) => row.serial_number === meter.serial_number))
}

async function checkAndUpdateAssignmentsForAvailableMeters(client, stekASDClient, assignments) {
	const { dateFrom, dateTo } = getDateTimePeriod()
	const updatedAssignments = []
	const strSerialNumbers = assignments.map(({ meter_serial_number }) => `'${ meter_serial_number }'`).join(',')
	const query = `select max(x.date_time) as last_date_time, x.serial_number, max(x.value) as last_value from (
										select ap.ЗавНомер as serial_number,
									        data."ДатаВремя" as date_time,
											data."Значение" as value
											from stack."АСД Приборы" ap,
											stack."АСД Каналы" ac,
											stack."АСД ДанНаСут" data
											where ap.ЗавНомер in (${ strSerialNumbers })
											and ap.row_id = ac.Прибор
											and ac."ТипКанала" = 1000
											and ac.row_id = data.Канал
											and data."ДатаВремя" between '${ dateFrom }' and '${ dateTo }'
											) x
											group by x.serial_number`

	const responseASD = await stekASDClient.query(query)
	const assignmentsToUpdate = assignments
		.filter(({ meter_serial_number }) => responseASD.rows.find((row) => row.serial_number === meter_serial_number))
		.map((assignment) => ({ ...assignment, serial_number: assignment.meter_serial_number }))
	
	if (!assignmentsToUpdate.length) {
		return updatedAssignments
	}
	const assignmentsWithLastData = await getEntityLastData(stekASDClient, assignmentsToUpdate)

	for (const { id, status, last_date_time, last_data_date } of assignmentsWithLastData) {
		const assignmentLastDataDate = new Date(last_data_date)
		const lastDataDate = new Date(last_date_time)
		
		if (lastDataDate > assignmentLastDataDate &&
				([ AssignmentStatus.REGISTERED, AssignmentStatus.IN_WORK ].includes(status))) {
			const isRegistered = status === AssignmentStatus.REGISTERED
			await createAssignmentEvent(
				client,
				isRegistered ? AssignmentEventType.CLOSED_AUTO : AssignmentEventType.SYSTEM_ACTION,
				id,
				null,
				`Последние данные получены ${ formatDateTimeForUser(last_date_time, false) }`,
				isRegistered ? ASSIGNMENT_EVENT_CLOSE_REASON_NOW_AVAILABLE : null
			)
			const updatedAssignment = await executeQuery(client,
				`update assignment
							set last_data_date = '${ formatDateTime(last_date_time) }',
							status = ${ isRegistered ? AssignmentStatus.CLOSED_AUTO : status },
							old_last_data_date = '${ formatDateTime(last_data_date) }'
							where id = ${ id } returning *`)
			
			const [ assignment ] = updatedAssignment
			updatedAssignments.push({ assignment, status: isRegistered ? 'closed' : 'updated' })
		}
	}
	
	return updatedAssignments
}

async function getRTCSimCardsData() {
	const httpsAgent = new Agent({ rejectUnauthorized: false })
	let authResponse = await axios.get('http://m2m.rt.ru/openapi/v1/tokens-stub-m2m/get?',
		{
			params: { login: rtc.login, password: rtc.password },
			httpsAgent: httpsAgent
		}
	)
	
	const { authToken } = authResponse.data
	//console.log(!authToken ? 'Токен авторизации для API ростелекома не получен' : `Токен получен: ${ authToken }`)
	const simResponse = await axios.post(
		'https://m2m.rt.ru/openapi/v1/M2M/SIMCards/search?', {},
		{
			params: { authToken: authToken, limit: 1 },
			httpsAgent: httpsAgent
		}
	)
	const { listInfo } = simResponse.data
	const pageCount = Math.trunc(listInfo.count / 1000)
	console.log(`Количество записей = ${ listInfo.count }`)
	
	const simCards = await Promise.all(Array
		.from({ length: pageCount + 1 }, (_, index) => index)
		.map(async (page) => {
			const { data } = await axios.post(
				'https://m2m.rt.ru/openapi/v1/M2M/SIMCards/search?', {},
				{
					params: {
						authToken: authToken,
						limit: 1000,
						offset: page * 1000
					},
					httpsAgent: httpsAgent
				})
			const { SIMCards } = data
			return SIMCards
		}))
	
	return simCards
		.reduce((totalSimCards, simCardsFromPage) => totalSimCards.concat(simCardsFromPage), [])
		.map(({ simcardStatus, MSISDN }) => ({ simStatus: simcardStatus.SIMCardStatusId, simPhone: MSISDN }))
}

async function createAssignmentEvent(client, eventType, assignmentId, ownerId, description, closeReason) {
	const owner = ownerId ? ownerId : null
	const desc = description ? `'${ description }'` : null
	const reason = closeReason ? closeReason : null
	return await executeQuery(client,
								`insert into assignment_event (
										created,
										type,
										owner_id,
										description,
										close_reason,
										assignment_id)
									values (
										'${ getCurrentDateTime() }',
										${ eventType },
										${ owner },
										${ desc },
										${ reason },
										${ assignmentId }) returning *`)
}

async function executeQuery(client, query) {
	console.log(query)
	const { rows } = await client.query(query)
	return rows
}

function createScheduleRule() {
	let schedule = new Schedule.RecurrenceRule()
	const { days, hour, minute } = UpdateScheduleRule
	schedule.dayOfWeek = days
	schedule.hour = hour
	schedule.minute = minute
	Schedule.scheduleJob(schedule, async () => updateServiceSystem())
}

async function updateServiceSystem() {
	let updatedAssignments = []
	let newAssignments = []

	const client = await pgPool.connect()
	const stekASDClient = await pgStekASDPool.connect()
	
	try {
		const assignments = await executeQuery(client,'select * from assignment')
		if (assignments.length) {
			//поиск данных за период 3 дня и обновление последних данных счетчика для
			//зарегистрированных или находящихся в работе поручений
			updatedAssignments = await checkAndUpdateAssignmentsForAvailableMeters(client, stekASDClient, assignments)
		}
		const nonActiveMeters = await getNonActiveMeters(client, stekASDClient)
		const nonActiveMetersWithLastData = await getEntityLastData(stekASDClient, nonActiveMeters)
		const simCardsUpdateData = await getRTCSimCardsData()
		//по полученным не активным счетчикам и их данным регистрация новых
		// или переоткрытие старых закрытых поручений со статусами CLOSE_REASON_TYPES_NEEDS_TO_CALLBACK
		newAssignments = await insertNewAssignments(
			client,
			nonActiveMetersWithLastData,
			assignments,
			simCardsUpdateData
		)
	
		const updateData = updatedAssignments.concat(newAssignments)
		await executeQuery(client,`insert into assignment_log (created, data)
													values ('${ getCurrentDateTime() }', '${ JSON.stringify(updateData) }')`)
	} catch (e) {
		console.log(e)
		await executeQuery(client,`insert into assignment_log (created, data)
													values ('${ getCurrentDateTime() }', '${ JSON.stringify(e.message) } ${ JSON.stringify(e.message) }')`)
	} finally {
		client.release()
		stekASDClient.release()
	}
}

async function createAssignmentWithLastData(client, stekASDClient, apiRes, authResult, serialNumber, type) {
	const assignment = await executeQuery(client, `select * from assignment
											where meter_serial_number = '${ serialNumber }' and meter_type = ${ type }`)
	const [ foundAssignment ] = assignment
	if (foundAssignment) {
		apiRes.status(400).send('поручение с такими данными уже существует')
		return
	}
	
	let meter = await executeQuery(client, `select * from meter_reg
														where serial_number = '${ serialNumber }' and type = ${ type }`)
	if (!meter.length) {
		apiRes.status(400).send('счетчик не найден в базе программирования')
		return
	}
	
	[ meter ] = meter

	const query = `select serial_number, max(to_char(date_time, 'YYYY-MM-DD HH24:MI:SS')) as last_date_time from (
														select ap.ЗавНомер as serial_number,
														data."ДатаВремя" as date_time,
														data."Значение" as value
														from stack."АСД Приборы" ap,
														stack."АСД Каналы" ac,
														stack."АСД ДанНаСут" data
														where ap.ЗавНомер = '${ serialNumber }'
														and ap.row_id = ac.Прибор
														and ac."ТипКанала" = 1000
														and ac.row_id = data.Канал
														order by data."ДатаВремя" desc) x group by serial_number`
	
	const { rows } = await stekASDClient.query(query)
	
	if (rows.length) {
		const [ lastDataDate ] = rows
		meter.last_date_time = lastDataDate.last_date_time
	}
	const simCardsUpdateData = await getRTCSimCardsData()
	return await createAssignment(client, meter, authResult.id, simCardsUpdateData)
	// const {
	// 	ip_address,
	// 	personal_account,
	// 	customer_address,
	// 	customer_phone,
	// 	customer_email,
	// 	phone,
	// 	contact,
	// 	port,
	// 	status,
	// 	address
	// } = meter
	// const contacts = `'${ customer_phone ? customer_phone : '' } ${ customer_email ? customer_email : '' }'`.trim()
	// const customerContacts = contacts ? contacts : null
	// const customerAddress = customer_address ? `'${ customer_address }'` : null
	// const personalAccount = personal_account ? `'${ personal_account }'` : null
	// const meterAddress = address ? `'${ address }'` : null
	// const meterPhone = phone ? `'${ phone }'` : null
	//
	// const simCardsUpdateData = await getRTCSimCardsData()
	// const simCard = simCardsUpdateData.find(({ simPhone }) => simPhone === phone)
	// const finalSimStatus = simCard && simCard.simStatus !== status ? simCard.simStatus: status
	//
	// const createdAssignment = await executeQuery(client,
	// 								`insert into assignment (
	// 											owner_id,
	// 											meter_type,
	// 											meter_serial_number,
	// 											meter_ip_address,
	// 											meter_phone,
	// 											meter_sim_status,
	// 											customer_personal_account,
	// 											customer_address,
	// 											customer_contacts,
	// 											last_data_date,
	// 											created,
	// 											status,
	// 											meter_address,
	// 											meter_port,
	// 											meter_contact)
	// 										values (
	// 											${ authResult.id },
	// 											${ type },
	// 											'${ serialNumber }',
	// 											${ ip_address },
	// 											${ meterPhone },
	// 											${ finalSimStatus },
	// 											${ personalAccount },
	// 											${ customerAddress },
	// 											${ customerContacts },
	// 											${ lastDataDateTime },
	// 											'${ getCurrentDateTime() }',
	// 											${ AssignmentStatus.REGISTERED },
	// 											${ meterAddress },
	// 											${ port },
	// 											${ contact }
	// 										) returning *`)
	//
	// return createdAssignment
}

async function createAssignment(client, meter, authResultId, simCardsUpdateData) {
	const {
		ip_address,
		personal_account,
		customer_address,
		customer_phone,
		customer_email,
		phone,
		contact,
		port,
		status,
		address,
		last_date_time,
		type,
		serial_number
	} = meter
	const contacts = `'${ customer_phone ? customer_phone : '' } ${ customer_email ? customer_email : '' }'`.trim()
	const customerContacts = contacts ? contacts : null
	const customerAddress = customer_address ? `'${ customer_address }'` : null
	const personalAccount = personal_account ? `'${ personal_account }'` : null
	const meterAddress = address ? `'${ address }'` : null
	const meterPhone = phone ? `'${ phone }'` : null
	const ownerId = authResultId ? authResultId : null
	const lastDataDateTime = last_date_time ? `'${ last_date_time } '` : null
	
	const simCard = simCardsUpdateData.find(({ simPhone }) => simPhone === phone)
	const finalSimStatus = simCard && simCard.simStatus !== status ? simCard.simStatus: status
	
	const createdAssignment = await executeQuery(client,
		`insert into assignment (
												owner_id,
												meter_type,
												meter_serial_number,
												meter_ip_address,
												meter_phone,
												meter_sim_status,
												customer_personal_account,
												customer_address,
												customer_contacts,
												last_data_date,
												created,
												status,
												meter_address,
												meter_port,
												meter_contact)
											values (
												${ ownerId },
												${ type },
												'${ serial_number }',
												${ ip_address },
												${ meterPhone },
												${ finalSimStatus },
												${ personalAccount },
												${ customerAddress },
												${ customerContacts },
												${ lastDataDateTime },
												'${ getCurrentDateTime() }',
												${ AssignmentStatus.REGISTERED },
												${ meterAddress },
												${ port },
												${ contact }
											) returning *`)
	
	return createdAssignment
}

function _validateAssignment(assignment) {
	const schema = {
		customerContacts: joi.string().empty('').allow(null),
		description: joi.string().empty('').allow(null),
		closeReason: joi.number().allow(null),
	}
	return joi.validate(assignment, schema);
}

function _validateAddAssignment(assignment) {
	const schema = {
		serialNumber: joi.string().required(),
		type: joi.number().required(),
	}
	return joi.validate(assignment, schema);
}
