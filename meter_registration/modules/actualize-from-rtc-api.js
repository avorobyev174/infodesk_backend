const { showRequestInfoAndTime, joi, executePGIQuery  } = require('../../utils'),
axios = require('axios'),
{ Agent } = require('https'),
parse = require('xml-parser'),
{ pgEnergoPool } = require("../../database/postgres/postgres-energo-db-connection"),
{ checkAuth } = require('../../login/login-api'),
{ rtc, beeline } = require('../../module-credentials')

module.exports = class actualizeFromRTCApi {
	constructor(app, module_name) {
		//Получение номера телефона, статуса из РТК
		app.get(`/api/${ module_name }/actualize-data-from-rtc`, async (apiReq, apiRes) => {
			showRequestInfoAndTime(`Регистрация счетчиков: запрос на предоставление данных для актуализации по API ростелекома`)

			if (!checkAuth(apiReq, apiRes))
				return

			try {
				const httpsAgent = new Agent({ rejectUnauthorized: false })
		
				let respAuth = await axios.get('http://m2m.rt.ru/openapi/v1/tokens-stub-m2m/get?',
					{
						params: {
							login: rtc.login,
							password: rtc.password
						},
						httpsAgent: httpsAgent
					}
				)

				//console.log(respAuth)
				const authToken = respAuth.data.authToken
				if (!authToken)
					console.log('Не получено токена авторизации для API ростелекома')
				else
					console.log(`Токен получен: ${ authToken }`)

				let simCardsAll = []

				let count = 0
				let res = await axios.post(
					'https://m2m.rt.ru/openapi/v1/M2M/SIMCards/search?', {},
					{
						params: {
							authToken: authToken,
							limit: 1
						},
						httpsAgent: httpsAgent
					}
				)

				count = Math.trunc(res.data.listInfo.count / 1000)
				//console.log(`Количество записей = ${ res.data.listInfo.count }`)

				for (let i = 0; i < count + 1; i++) {
					let resSimCards = await axios.post(
						'https://m2m.rt.ru/openapi/v1/M2M/SIMCards/search?', {},
						{
							params: {
								authToken: authToken,
								limit: 1000,
								offset: i * 1000
							},
							httpsAgent: httpsAgent
						}
					)

					simCardsAll = simCardsAll.concat(resSimCards.data.SIMCards)
				}

				console.log(`Получены данные о симкартах, размер: ${ simCardsAll.length }`)
				apiRes.send(simCardsAll)
			} catch (e) {
				if (e.response) {
					//console.log(e.response)
					console.log(`Запрос по API для получения симкарт завершился ошибкой:
                                    status = ${ e.response.status }
                                    text '${ e.response.statusText }'`)
					apiRes.status(400).send(e.response.statusText)
				} else {
					//console.log(e)
					console.log(`Запрос по API для получения симкарт завершился ошибкой: msg = ${ e.message }`)
					apiRes.status(501).send(e.message)
				}
			}
		})

		//сохранение данных после актуализации из РТК параметров счетчика
		app.put(`/api/${ module_name }/update-meter-from-rtc/:id`, async (apiReq, apiRes) => {
			const meterId = apiReq.params.id;

			showRequestInfoAndTime(`Регистрация счетчиков: запрос на акутализацию номера телефона и статуса счетчика с id = ${meterId}`)

			if (!checkAuth(apiReq, apiRes)) return

			const { error } = _validateActualizeMeter(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)

			const smsStatus = [5, 6, 7, 18, 20, 21, 22].includes(apiReq.body.type) ? 7 : 1 //МИРы не требуют смс

			const query = `update meters set (
                                            phone,
                                            status,
                                            sms_status
                                            )
                                        = (
                                            ${ apiReq.body.phone },
                                            ${ apiReq.body.status },
                                            ${ smsStatus }
                                        ) where id = ${ meterId } returning *`

			executePGIQuery(query, apiRes)
		})
		
		//отправка смс для регистрации счетчика
		app.post(`/api/${ module_name }/send-sms/:id`, async (apiReq, apiRes) => {
			const { error } = _validateSmsMeter(apiReq.body);
			if (error) return apiRes.status(400).send(error.details[0].message);
			
			apiReq.setTimeout(0) //чтобы браузер не слал повторный запрос по таймауту
			
			//console.log(apiReq.body)
			const meterId = apiReq.params.id;
			const serialNumber = apiReq.body.serial_number
			const phase = apiReq.body.phase
			const type = apiReq.body.type
			//let phone = [16].includes(type) ? '7' + apiReq.body.phone : '8' + apiReq.body.phone//ЭНЕРГОМЕРА телефон с 7
			let phone = '8' + apiReq.body.phone
			const port = phase === 1 ? 10006 : 10007
			
			showRequestInfoAndTime(`Регистрация счетчиков: запрос по API билайна на регистрацию счетчика по смс:
			                         серийный номер - ${ serialNumber },
			                         тип - ${ type },
			                         фазность - ${ phase},
			                         телефон - ${ phone }`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			if ([16].includes(type)) {
				/*const client = new net.Socket();
				let successProgramming = true
				
				client.connect(60001, '172.27.2.232', function() {
					console.log('Соединение установлено')
					phone += 'F'
					let phoneStr = '', i = 0
					
					while (i <= phone.length - 2) {
						phoneStr += [ ...phone.slice(i, i + 2) ].reverse().join('')
						i += 2
					}
					const delayTime = 1500
					console.log(phone, phoneStr)
					
					client.write('AT\r')
					
					_delay(delayTime).then(resolve => {
						console.log('1 command')
						client.write('AT+CPMS="SM"\r')
						return _delay(delayTime)
					}).then(resolve => {
						client.write('AT+CMGF=0\r')
						console.log('2 command')
						return _delay(delayTime)
					}).then(resolve => {
						client.write('AT+CMGL=4\r')
						console.log('3 command')
						return _delay(delayTime)
					}).then(resolve => {
						client.write('AT+CPMS="SM"\r')
						console.log('4 command')
						return _delay(delayTime)
					}).then(resolve => {
						client.write('AT+CMGF=0\r')
						console.log('5 command')
						return _delay(delayTime)
					}).then(resolve => {
						client.write('AT+CMGS=115\r')
						console.log('6 command')
						return _delay(delayTime)
					}).then(resolve => {
						console.log('7 command')
						client.write(
							'0001000B91' + phoneStr +
							'000866013132333435363738022802581723696E7465726E65742E74656C65322E727500172474656C653' +
							'200172574656C653200172638302E3234342E33342E3131380017272710172E18172CFF05A0172DFF001E' +
							'205B6E61726F646D6F6E2E727500172203170300' + String.fromCharCode(26)
						)
						return _delay(10000)
					}).then(resolve => {
						if (successProgramming) {
							console.log('Смс отправлено');
							const query = `update meters set sms_status = 8 where id = ${ meterId } returning *`
							executePGIQuery(query, apiRes)
						} else {
							console.log('Что то пошло не так при отправке команд АТ');
						}
					})
				})
				
				client.on('data', function(data) {
					console.log('Ответ от сервера: ' + data)
					if (data && data.toString().trim() === 'ERROR') {
						successProgramming = false
						return apiRes.status(400).send('Что то пошло не так при отправке команд АТ')
					}
				})
				
				client.on('close', function() {
					console.log('Соединение закрыто')
				})
				
				client.on('error', function(err) {
					console.error('Ошибка соединения: ' + err);
					console.error(new Error().stack);
				})*/
				
				let isConnected = false, messageId = 0
				
				const session = smpp.connect( {
					url: 'smpp://smpp.beeline.amega-inform.ru:8077'
				})
				
				session.on('connect', () => {
					isConnected = true;
					
					console.log(`smpp соединение установлено`)
					session.bind_transceiver({
						system_id: beeline.user,
						password:  beeline.password
					}, pdu => {
						console.log('pdu получен')
						//console.log(pdu)
						let phoneOriginal = phone, phoneStr = '', i = 0
						phone += 'F'
						
						while (i <= phone.length - 2) {
							phoneStr += [ ...phone.slice(i, i + 2) ].reverse().join('')
							i += 2
						}
						
						console.log(phoneOriginal, phone, phoneStr)
						let message = '0001000B91' + phoneStr +
							'000866013132333435363738022802581723696E7465726E65742E74656C65322E727500172474656C653' +
							'200172574656C653200172638302E3234342E33342E3131380017272710172E18172CFF05A0172DFF001E' +
							'205B6E61726F646D6F6E2E727500172203170300'
						
						console.log(message)
						console.log('Addr: ' + phoneOriginal)
						session.submit_sm({
							destination_addr: phoneOriginal,
							short_message: message,
							registered_delivery: 1
							
						}, function(pdu) {
							console.log('pdu submit_sm получен')
							if (pdu.command_status === 0) {
								console.log(pdu)
								messageId = pdu.message_id
								isConnected = false;
								//session.destroy()
								const query = `update meters set sms_status = 8, sms_id = ${ messageId } where id = ${ meterId } returning *`
								executePGIQuery(query, apiRes)
							}
						})
						
					})
				})
				
				session.on('close', () => {
					console.log('smpp соединение разъединено')
					if (isConnected) {
						session.connect();
					}
				})
				
				session.on('error', error => {
					console.log('smpp ошибка', error)
					isConnected = false;
				})
				
				session.on('deliver_sm', function(pdu) {
					let msg = pdu.short_message.message;
					let resArr = msg.split(' ')
					let date = resArr[4].split(':')
					if (date[1].startsWith('23011213')) {
						console.log('-----------deliver_sm-------')
						console.log(msg);
					}
				});
				
			} else {
				const message = `###!0!21!2!80.244.34.118!${ port }!15!${ serialNumber }!tele2!tele2!internet.tele2.ru!0,0!!!`
				
				try {
					const response = await axios.get(
						'https://beeline.amega-inform.ru/sms_send/?',
						{
							params: {
								user: beeline.user,
								pass: beeline.password,
								action: 'post_sms',
								target: phone,
								message: message
							}
						}
					)
					//let xml = '<?xml version=\'1.0\' encoding=\'UTF-8\'?><output><RECEIVER AGT_ID="11206" DATE_REPORT="27.04.2022 13:20:39"/><result sms_group_id="22840432649"><sms id="22840432650" smstype="SENDSMS" phone="+79000275775" sms_res_count="1"><![CDATA[123]]></sms></result></output>'
					//console.log(response)
					//let obj = parse(xml);
					//console.log(inspect(obj, { colors: true, depth: Infinity }));
					console.log("Получен ответ об отправке смс, происходит парсинг");
					let obj = parse(response.data);
					let smsId = null
					if (obj && obj.root && obj.root.children && obj.root.children.length > 1) {
						const result = obj.root.children[1]
						if (result.children && result.children.length > 0) { // noinspection TypeScriptValidateTypes
							smsId = result.children[0].attributes.id
						}
					} else {
						console.log('Не удалось распарсить ответ по API билайна (smsId)')
					}
					
					console.log(`Данные распарсены: ид смс = ${ smsId }`)
					
					const query = `update meters set
                                            (sms_id, sms_status)
                                             =
                                            (${ smsId }, 2)
                                             where id = ${ meterId } returning *`
					
					executePGIQuery(query, apiRes)
				} catch (e) {
					console.log(e)
					if (e.response) {
						const { status, statusText } = e.response
						console.log(`Запрос по API билайна на регистрацию завершился с ошибкой : статус = ${ status }, сообщение ${ statusText }`)
					} else {
						console.log(`Ошибка: сообщение = ${ e.message }`)
					}
				}
			}
		})
		
		//получение статуса отправленной смс
		app.post(`/api/${ module_name }/check-sms/:meterId`, async (apiReq, apiRes) => {
			const { error } = _validateCheckSms(apiReq.body);
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			const meterId = apiReq.params.meterId
			const smsId = apiReq.body.sms_id
			const type = apiReq.body.type
			const serialNumber = apiReq.body.serial_number
			
			if (!meterId) {
				return apiRes.status(400).send('Не указан id счетчика');
			}
			
			showRequestInfoAndTime(`Регистрация счетчиков: запрос по API билайна на статус смс:
                                                                     ид счетчика - ${ meterId },
                                                                     серийный номер - ${ serialNumber },
                                                                     тип - ${ type },
                                                                     смс ид - ${ smsId }`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			if ([16].includes(type)) {
				const query = `select "DirectAccessPort" from public."Devices" where "SerialNumber" = '${ serialNumber }'`
				
				pgEnergoPool.connect((connErr, client, done) => {
					if (connErr) apiRes.status(400).send(connErr.detail)
					
					client
						.query(query)
						.then(
							resolve => {
								if (!resolve.rows.length)
									return apiRes.send([{ id: meterId, message: `Счетчик ${ serialNumber } еще не вышел на связь, повторите попытку позже` }])
								
								const port = resolve.rows[0].DirectAccessPort
								//console.log(`port = ${ port }`)
								if (port) {
									const query = `update meters set (
																		sms_status,
																	    port
																    ) = (
																		3,
																	    ${ resolve.rows[0].DirectAccessPort }
																    ) where id = ${ meterId } returning *`
									executePGIQuery(query, apiRes)
								} else {
									done()
									apiRes.send([{ id: meterId, message: `Что то пошло не так при определении порта счетчика ${ serialNumber }` } ])
								}
							}
						)
						.catch(e => {
							done()
							console.log(`Запрос (${ query }). Ошибка: ${ e }`)
							return apiRes.status(400).send(e.detail)
						})
				})
			} else {
				try {
					if (!smsId) return apiRes.status(400).send('Не указано sms_id счетчика')
					
					const response = await axios.get(
						'https://beeline.amega-inform.ru/sms_send/?',
						{
							params: {
								user: beeline.user,
								pass: beeline.password,
								action: 'status',
								sms_id: smsId
							}
						}
					)
					
					//console.log(response.data)
					let status = -1
					
					const indexOpenTag = response.data.indexOf('<SMSSTC_CODE>') + 13
					const indexCloseTag = response.data.indexOf('</SMSSTC_CODE>')
					
					//console.log(indexOpenTag, indexCloseTag)
					
					const smsStatus = response.data.substring(indexOpenTag, indexCloseTag)
					//console.log(smsStatus)
					
					switch (smsStatus) {
						case 'delivered':
							status = 3
							console.log('Доставлено')
							break
						case 'queued':
							status = 2
							console.log('В очереди')
							break
						case 'wait':
							status = 5
							console.log('Перадано оператору')
							break
						case 'failed':
							status = 6
							console.log('Запрещено посылать сообщение с тем же текстом тому же адресату в течение 20 минут')
							break
						case 'not_delivered':
							console.log('Не доставлено')
							status = 4
							break
						default:
							return apiRes.status(400).send('Что то пошло не так при запросе статуса смс из кабинета билайн')
					}
					
					const query = `update meters set sms_status = ${ status } where id = ${ meterId } returning *`
					
					executePGIQuery(query, apiRes)
				} catch (e) {
					console.log(e)
					console.log(`Ошибка: сообщение = ${ e.message }`)
					return apiRes.status(400).send(e.message);
				}
			}
		})
	}
}

function _validateActualizeMeter(meter) {
	const schema = {
		phone: joi.number().required(),
		status: joi.number().required(),
		type: joi.number().required()
	}
	return joi.validate(meter, schema);
}

function _validateSmsMeter(meter) {
	const schema = {
		serial_number: joi.string().required(),
		phase: joi.number().required(),
		phone: joi.number().required(),
		type: joi.number().required()
	}
	return joi.validate(meter, schema);
}

function _validateCheckSms(meter) {
	const schema = {
		sms_id: joi.number().allow(null).empty(''),
		type: joi.number().required(),
		serial_number: joi.number().required()
	}
	return joi.validate(meter, schema);
}

