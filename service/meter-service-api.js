const { pgPool } = require("../database/postgres/postgres-db-connection")
const { pgStekASDPool } = require("../database/postgres/postgres-stek-asd-db-connection")
const { getDateTime, formatDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'meter-service'
const DAY_DEPTH = 3
const { AssignmentEventType } = require("../const.js")

module.exports = class MeterServiceApi {
	constructor(app) {
		//Получение списка поручений
		app.get(`/api/${ module_name }/assignments`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			showRequestInfoAndTime(`Поручения: запрос на список поручений`)
			
			const dateNow = new Date()
			const dayNow = dateNow.getDate()
			const monthNow = dateNow.getMonth() + 1
			const yearNow = dateNow.getFullYear()
			let dateFrom = new Date(yearNow, monthNow - 1, dayNow - DAY_DEPTH)
			dateFrom = `${ dateFrom.getFullYear() }-${ dateFrom.getMonth() + 1 }-${ dateFrom.getDate() }`
			const dateTill = `${ yearNow }-${ monthNow }-${ dayNow }`
			
			console.log(dateFrom)
			console.log(dateTill)
			
			const client = await pgPool.connect()
			const stekASDClient = await pgStekASDPool.connect()
			try {
				const meters = await client.query('select * from meter_reg where in_pyramid = 1')
				const metersArray = meters.rows
				const strSerialNumbers = metersArray.map((meter) => `'${ meter.serial_number }'`).join(',')
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
											and data."ДатаВремя" between '${ dateFrom }' and '${ dateTill }'
											) x
											group by x.serial_number`
				const responseASD = await stekASDClient.query(query)
				const nonActiveMeters = metersArray
					.filter((meter) => !responseASD.rows.find((row) => row.serial_number === meter.serial_number))
				
				const nonActiveStrSerialNumbers = nonActiveMeters.map((meter) => `'${ meter.serial_number }'`).join(',')
				
				const queryLastData = `select serial_number, max(to_char(date_time, 'YYYY-MM-DD HH24:MI:SS')) as last_date_time from (
													select ap.ЗавНомер as serial_number,
													data."ДатаВремя" as date_time,
													data."Значение" as value
													from stack."АСД Приборы" ap,
													stack."АСД Каналы" ac,
													stack."АСД ДанНаСут" data
													where ap.ЗавНомер in (${ nonActiveStrSerialNumbers })
													and ap.row_id = ac.Прибор
													and ac."ТипКанала" = 1000
													and ac.row_id = data.Канал
													order by data."ДатаВремя" desc) x group by serial_number`
				
				const metersASDWithLastData = await stekASDClient.query(queryLastData)
				const nonActiveMetersWithLastData = nonActiveMeters.map((meter) => {
					const nonActiveMeterLastData = metersASDWithLastData.rows.filter((row) => row.serial_number === meter.serial_number)
					if (nonActiveMeterLastData.length) {
						const [ lastData ] =  nonActiveMeterLastData
						const { last_date_time } = lastData
						return { ...meter, last_date_time }
					}
					return meter
				})
				
				let assignments = await executeQuery(client,'select * from assignment')

				for (const {
					type,
					serial_number,
					ip_address,
					phone,
					status,
					personal_account,
					customer_address,
					customer_phone,
					customer_email,
					last_date_time
				} of nonActiveMetersWithLastData) {
					const assignment = assignments
						.find(({ meter_type, meter_serial_number }) =>
							meter_serial_number === serial_number && meter_type === type)
					
					if (!assignment) {
						const lastDataDateTime = last_date_time ? `'${ last_date_time }'` : null
						const customerContacts = `${ customer_phone } ${ customer_email }`.trim()
						const assignment = await executeQuery(client,`insert into assignment (
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
															status)
														values (
															${ type },
															'${ serial_number }',
															${ ip_address },
															'${ phone }',
															${ status },
															'${ personal_account }',
															'${ customer_address }',
															'${ customerContacts }',
															${ lastDataDateTime },
															'${ getDateTime() }',
															0) returning *`)
						
						const [ newAssignment ] = assignment
						const { id } = newAssignment
						await createAssignmentEvent(client, AssignmentEventType.REGISTER, id)
					}
				}

				assignments = await executeQuery(client,'select * from assignment')
				apiRes.send(assignments)
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
				stekASDClient.release()
			}
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
			const { customer_contacts, status } = apiReq.body
			const client = await pgPool.connect()
			try {
				let query = ''
				if (customer_contacts) {
					query = `update assignment set customer_contacts = '${ customer_contacts }' where id = ${ assignmentId } returning *`
				} else if (status) {
					const { value, reason } = status
					query = `update assignment set status = ${ value }, status_reason = ${ reason } where id = ${ assignmentId } returning *`
					await createAssignmentEvent(client, AssignmentEventType.CLOSE, assignmentId)
				} else {
					query = `update assignment set owner_id = ${ authResult.id }, status = 1 where id = ${ assignmentId } returning *`
					await createAssignmentEvent(client, AssignmentEventType.EDIT, assignmentId)
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
			executePGIQuery(`select * from assignment_event where assignment_id = ${ assignmentId }`, apiRes)
		})
	}
}

async function createAssignmentEvent(client, eventType, assignmentId) {
	await executeQuery(client,`insert into assignment_event (
										created,
										type,
										assignment_id)
									values (
										'${ getDateTime() }',
										${ eventType },
										${ assignmentId })`)
}

async function executeQuery(client, query) {
	console.log(query)
	const { rows } = await client.query(query)
	return rows
}

function _validateAssignment(assignment) {
	const schema = {
		customer_contacts: joi.string().empty(),
	}
	return joi.validate(assignment, schema);
}
