const { showRequestInfoAndTime, joi, executePGIQuery } = require('../../utils'),
{ pgStekPool } = require("../../database/postgres/postgres-stek-db-connection"),
{ pgPool } = require("../../database/postgres/postgres-db-connection"),
{ checkAuth } = require('../../login/login-api')

module.exports = class actualizeFromStekApi {
	constructor(app, module_name) {
		//получение данных из СТЭКа
		app.get(`/api/${ module_name }/actualize-data-from-stek/`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			apiReq.setTimeout(0) //чтобы браузер не слал повторный запрос по таймауту(большое время ожидания)
			
			showRequestInfoAndTime(`Регистрация счетчиков: запрос на получение данных из стека`)
			
			const query = `select serial_number from meter_reg where personal_account is null`
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) {
					apiRes.status(400).send(connErr.detail)
				}
				
				client
					.query(query)
					.then(
						resolve => {
							const serialNumbers = resolve.rows
							console.log(`Найдено ${ serialNumbers.length } счетчиков`)
							const strSerialNumbers = serialNumbers.map(sn => sn.serial_number).join(',')
							
							pgStekPool.connect((stekConnErr, stekClient, stekDone) => {
								if (stekConnErr) {
									apiRes.status(400).send(stekConnErr.detail)
								}
								
								stekClient
									.query(`select * from stack.grmpontinfo('${ strSerialNumbers }', '')`)
									.then(
										resolve => {
											const metersData = resolve.rows.filter(row => parseInt(row.kftt) > 0)
											console.log('Запрос на получение информации выполнен успешно (' + metersData.length + ' строк)')
											stekDone()
											done()
											apiRes.status(200).send(metersData)
										})
									.catch(
										error => {
											stekDone()
											done()
											console.log(`Запрос (${ query }). Ошибка: ${ error }`)
											apiRes.status(400).send(error.routine)
										}
									)
							})
						}
					).catch(
						error => {
							done()
							console.log(`Запрос (${ query }). Ошибка: ${ error }`)
							apiRes.status(400).send(error.routine)
						}
					)
				
			})
		})
		
		//сохранение данных после актуализации из СТЭКа
		app.put(`/api/${ module_name }/update-meter-from-stek/:id`, (apiReq, apiRes) => {
			const meterId = apiReq.params.id;
			showRequestInfoAndTime(
				`Регистрация счетчиков: запрос на акутализацию данных из стека по счетчику с id = ${ meterId }`
			)
			
			const { error } = _validateActualizeMeterStek(apiReq.body);
			
			if (error) return apiRes.status(400).send(error.details[0].message);
			
			const account = apiReq.body.personal_account
			const customer = apiReq.body.customer === null ? '' : apiReq.body.customer
			const address = apiReq.body.customer_address === null ? '' : apiReq.body.customer_address
			const phone = apiReq.body.customer_phone === null ? '' : apiReq.body.customer_phone
			const email = apiReq.body.customer_email === null ? '' : apiReq.body.customer_email
			const type = apiReq.body.customer_type === null ? '' : apiReq.body.customer_type
			const kftt = apiReq.body.kftt === null ? 1 : apiReq.body.kftt
			
			const query = `update meter_reg set (
                                            personal_account,
                                            customer,
                                            customer_address,
                                            customer_phone,
                                            customer_email,
                                            customer_type,
                                            kftt
                                            )
                                        = (
                                            '${ account }',
                                            '${ customer }',
                                            '${ address }',
                                            '${ phone }',
                                            '${ email }',
                                            '${ type }',
                                            '${ kftt }'
                                        ) where id = ${ meterId } returning *`
			
			executePGIQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/actualize-data-from-stek/:id`, async (apiReq, apiRes) => {
			const meterId = apiReq.params.id;
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			showRequestInfoAndTime(`Регистрация счетчиков: запрос на получение данных из стека с id = ${ meterId }`)
			
			const query = `select serial_number from meter_reg where id = ${ meterId } and personal_account is null`
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) {
					apiRes.status(400).send(connErr.detail)
				}
				
				client
					.query(query)
					.then(
						resolve => {
							console.log(`Найдено ${ resolve.rows.length } счетчиков`)
							
							if (!resolve.rows.length) {
								done()
								return apiRes.status(400).send(
									`Счетчика с id = ${ meterId } и пустым лицевым номером не найдено`
								)
							}
							
							const serialNumber = resolve.rows[0].serial_number
							
							pgStekPool.connect((stekConnErr, stekClient, stekDone) => {
								if (stekConnErr) {
									return apiRes.status(400).send(stekConnErr.detail)
								}
								
								stekClient
									.query(`select * from stack.grmpontinfo('${ serialNumber }', '')`)
									.then(
										resolve => {
											if (!resolve.rows.length) {
												stekDone()
												done()
												apiRes.status(400).send(
													`Счетчика с id = ${ meterId } не найдено в базе СТЭКа`
												)
											}
											
											const meterData = resolve.rows.filter(row => parseInt(row.kftt) > 0)[0]
											console.log('Запрос на получение информации выполнен успешно')
											console.log(meterData)
											const updQuery = `update meter_reg set (
					                                            personal_account,
					                                            customer,
					                                            customer_address,
					                                            customer_phone,
					                                            customer_email,
					                                            customer_type,
					                                            kftt
					                                            )
					                                        = (
					                                            '${ meterData.personal_account }',
					                                            '${ meterData.customer }',
					                                            '${ meterData.customer_address }',
					                                            NULLIF('${ meterData.customer_phone }', 'null'),
					                                            NULLIF('${ meterData.customer_email }', 'null'),
					                                            '${ meterData.customer_type }',
					                                            '${ meterData.kftt }'
					                                        ) where id = ${ meterId } returning *`
											console.log(updQuery)
											return client.query(updQuery)
										})
									.then(
										resolve => {
											apiRes.status(200).send(resolve.rows[0])
											stekDone()
											done()
										}
									)
									.catch(
										error => {
											stekDone()
											done()
											console.log(`Запрос (${ query }). Ошибка: ${ error }`)
											apiRes.status(400).send(error.routine)
										}
									)
							})
						}
					).catch(
					error => {
						done()
						console.log(`Запрос (${ query }). Ошибка: ${ error }`)
						apiRes.status(400).send(error.routine)
					}
				)
				
			})
		})
	}
}

function _validateActualizeMeterStek(meter) {
	const schema = {
		personal_account: joi.string().required(),
		customer: joi.string().allow(null).empty(''),
		customer_address: joi.string().allow(null).empty(''),
		customer_phone: joi.string().allow(null).empty(''),
		customer_email: joi.string().allow(null).empty(''),
		customer_type: joi.string().allow(null).empty(''),
		kftt: joi.string().allow(null).empty('')
	}
	return joi.validate(meter, schema);
}