const { showRequestInfoAndTime, joi, executePGIQuery, getDateTime } = require('../../utils'),
{ pgStekPool } = require("../../database/postgres/postgres-stek-db-connection"),
{ pgPool } = require("../../database/postgres/postgres-db-connection"),
{ checkAuth } = require('../../login/login-api')

module.exports = class refreshFromStekApi {
	constructor(app, module_name) {
		//обновление данных из СТЭКа для пирамиды
		app.get(`/api/${ module_name }/refresh-data-from-stek/`, async (apiReq, apiRes) => {
				if (!checkAuth(apiReq, apiRes)) {
					return
				}
				apiReq.setTimeout(0)
				
				const client = await pgPool.connect()
				const stekClient = await pgStekPool.connect()
				try {
					const { rows } = await client.query(`select * from meter_reg where in_pyramid = 1 order by random() limit 10000`)
					const strSerialNumbers = rows.map((row) => row.serial_number).join(',')
					const meterMap = new Map()
					rows.forEach((row) => meterMap.set(row.serial_number, row))
					const query = `select * from stack.grmpontinfo('${ strSerialNumbers }', '')`
					const response = await stekClient.query(query)
					const meters = response.rows
					console.log('Запрос на получение информации выполнен успешно (' + meters.length + ' строк)')
					
					let dublData = []
					let noExistData = []
					let refreshMeterMap = new Map()
					
					meters.forEach((meter) => {
						if (!meterMap.has(meter.serial_number)) {
							noExistData.push(meter)
						} else {
							if (refreshMeterMap.has(meter.serial_number)) {
								dublData.push([ refreshMeterMap.get(meter.serial_number), meter ])
								refreshMeterMap.delete(meter.serial_number)
							} else {
								refreshMeterMap.set(meter.serial_number, meter)
							}
						}
					})
					
					console.log(`Присланные данные: ${ refreshMeterMap.size }`)
					console.log(`Дублирующие данные: ${ dublData.length }`)
					console.log(`Неизвестные данные: ${ noExistData.length }`)
					
					let diff = [];
					[ ...meterMap.keys() ].forEach((serialNumber) => {
						let oldMeterData = meterMap.get(serialNumber)
						
						if (refreshMeterMap.has(serialNumber)) {
							let newMeterData = refreshMeterMap.get(serialNumber)
							
							//Исключаем ОДУ
							if (oldMeterData.personal_account !== '99999999999'
								&& (oldMeterData.personal_account !== newMeterData.personal_account
									|| oldMeterData.customer_address !== newMeterData.customer_address
									|| oldMeterData.kftt !== parseInt(newMeterData.kftt)
								))
								diff.push({ oldMeterData, newMeterData, diff: 'diff' })
						} else
							diff.push({ oldMeterData, diff: 'not_exist' })
						
					})
					
					apiRes.status(200).send({ diff, total: meterMap.size })
				} catch (e) {
					console.log(e)
					apiRes.status(400).send(e.message || e.routine)
				} finally {
					client.release()
					stekClient.release()
				}
			})
	
		//сохранение данных после обновления из СТЭКа
		app.put(`/api/${ module_name }/save-refresh-meter-data-from-stek/:id`,async  (apiReq, apiRes) => {
			const meterId = apiReq.params.id
			showRequestInfoAndTime(`Регистрация счетчиков: запрос на сохранение именненных данных из стека по счетчику с id = ${meterId}`)
			const { error } = _validateRefreshMeterStek(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!meterId) {
				return apiRes.status(400).send('не указан id счетчика')
			}

			const account = apiReq.body.personal_account === null ? '' : apiReq.body.personal_account
			const customer = apiReq.body.customer === null ? '' : apiReq.body.customer
			const address = apiReq.body.customer_address === null ? '' : apiReq.body.customer_address
			const phone = apiReq.body.customer_phone === null ? '' : apiReq.body.customer_phone
			const email = apiReq.body.customer_email === null ? '' : apiReq.body.customer_email
			const type = apiReq.body.customer_type === null ? '' : apiReq.body.customer_type
			const kftt = apiReq.body.kftt === null ? 1 : apiReq.body.kftt
			const data = apiReq.body.data
			
			const authResult = checkAuth(apiReq, apiRes)
			if (!authResult) {
				return
			}
			
			const query = `insert into meter_reg_broken (meter_id, reason, created, comment, data, acc_id)
                            values (${ meterId }, 2, '${ getDateTime() }', '', '${ data }', ${ authResult.id }) returning *`
			
			const client = await pgPool.connect()
			
			try {
				await client.query(query)
				const queryUpdate = `update meter_reg set (
	                                            personal_account,
	                                            customer,
	                                            customer_address,
	                                            customer_phone,
	                                            customer_email,
	                                            customer_type,
	                                            loaded,
	                                            in_pyramid,
	                                            kftt
	                                            )
	                                        = (
	                                            NULLIF('${ account }', ''),
	                                            '${ customer }',
	                                            '${ address }',
	                                            '${ phone }',
	                                            '${ email }',
	                                            '${ type }',
	                                            null,
	                                            0,
	                                            ${ kftt }
	                                        ) where id = ${ meterId } returning *`

				const response = await client.query(queryUpdate)
				return apiRes.status(200).send(response.rows)
			} catch (e) {
				return apiRes.status(400).send(e.message || e.routine)
			} finally {
				client.release()
			}
		})
	}
}

function _validateRefreshMeterStek(meter) {
	const schema = {
		personal_account: joi.string().allow(null).empty(''),
		customer: joi.string().allow(null).empty(''),
		customer_address: joi.string().allow(null).empty(''),
		customer_phone: joi.string().allow(null).empty(''),
		customer_email: joi.string().allow(null).empty(''),
		customer_type: joi.string().allow(null).empty(''),
		kftt: joi.string().allow(null).empty(''),
		data: joi.string().allow(null).empty(''),
	}
	return joi.validate(meter, schema);
}