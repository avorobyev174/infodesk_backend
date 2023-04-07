const { pgPool } = require("../database/postgres/postgres-db-connection"),
	{ getOraConnectionUit, getOraConnectionCnt } = require("../database/oracle/oracle-db-connection"),
	{ pgStekASDPool } = require("../database/postgres/postgres-stek-asd-db-connection"),
	{ showRequestInfoAndTime, joi, executePGIQuery } = require('../utils'),
	{ checkAuth } = require('../login/login-api'),
	module_name = 'reports'

module.exports = class ReportsApi {
	constructor(app) {
		app.get(`/api/${ module_name }/alpha-last-time-data-report`, async (apiReq, apiRes) => {
			showRequestInfoAndTime(`Отчеты: запрос на отчет альфа центра по последнему опросу`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			const oraConn = await getOraConnectionCnt()
			const query = `SELECT d.n_sh SerialNo ,(Select Name_typ from spr_sh where d.typ_sh=spr_sh.typ_sh) DevType,
                            c.TXT nm ,b.TXT fid, d.Syb_rnk ,d.n_fid DevID ,d.N_OB UnitID,

                            (Select MAX(dd_mm_yyyy) from autoread a where  typ_pok = 1 and BKWH is not null and AKWH is not null
                            and SYB_RNK = d.SYB_RNK and N_OB = d.N_OB and  N_FID = d.N_FID) Dat

                            FROM FID b, obekt c, sh d, SYB_RNK i
                            where i.SYB_RNK = b.SYB_RNK and c.SYB_RNK = b.SYB_RNK and c.N_OB = b.N_OB and D.N_OB = C.N_OB and D.N_FID = B.N_FID
                            and c.SYB_RNK = d.SYB_RNK
                            and d.n_sh in (Select n_sh from sh) and

                            (Select MAX(dd_mm_yyyy) from autoread a where  typ_pok = 1 and BKWH is not null and AKWH is not null
                            and SYB_RNK = d.SYB_RNK and N_OB = d.N_OB and  N_FID = d.N_FID) >'01.01.2022'`
			
			oraConn.execute(query).then(
				result => {
					oraConn.close()
					apiRes.send(result.rows)
				},
				error => {
					oraConn.close()
					console.log(`Запрос (${ query }). Ошибка: ${ error }`);
					apiRes.status(400).send(error.detail);
				}
			)
		})
		
		app.get(`/api/${ module_name }/get-meter-loaded-count-by-customer-address`, (apiReq, apiRes) => {
			const plan = [
				{ address:'пр-кт Карла Маркса, д.196', count: 146 },
				{ address:'ул Суворова, д.125/3', count: 55 },
				{ address:'ул Суворова, д.133', count: 100 },
				{ address:'ул Завенягина, д.3', count: 105 },
				{ address:'ул Завенягина, д.16', count: 118 },
				{ address:'ул Завенягина, д.12', count: 186 },
				{ address:'проезд Сиреневый, д.10', count: 80 },
				{ address:'проезд Сиреневый, д.11/2', count: 111 },
				{ address:'проезд Сиреневый, д.15', count: 73 },
				{ address:'проезд Сиреневый, д.14/2', count: 72 },
				{ address:'проезд Сиреневый, д.27', count: 239 },
				{ address:'проезд Сиреневый, д.25', count: 67 },
				{ address:'проезд Сиреневый, д.24/2', count: 108 },
				{ address:'проезд Сиреневый, д.23', count: 107 },
				{ address:'проезд Сиреневый, д.26', count: 220 },
				{ address:'проезд Сиреневый, д.32', count: 108 },
				{ address:'ул Ворошилова, д.7', count: 96 },
				{ address:'ул Ворошилова, д.7/1', count: 70 },
				{ address:'ул Ворошилова, д.7/3', count: 70 },
				{ address:'ул Ворошилова, д.4', count: 128 },
				{ address:'ул Ворошилова, д.6', count: 107 },
				{ address:'ул Ворошилова, д.11', count: 286 },
				{ address:'ул Ворошилова, д.12', count: 106 },
				{ address:'ул Ворошилова, д.13', count: 69 },
				{ address:'ул Ворошилова, д.15', count: 109 },
				{ address:'ул Ворошилова, д.21', count: 136 },
				{ address:'ул Ворошилова, д.23', count: 151 },
				{ address:'ул Ворошилова, д.33', count: 64 },
				{ address:'ул Галиуллина, д.26/1', count: 57 },
				{ address:'ул Галиуллина, д.7', count: 100 },
				{ address:'ул Галиуллина, д.11', count: 99 },
				{ address:'ул Галиуллина, д.30', count: 131 },
				{ address:'ул Галиуллина, д.30/1', count: 71 },
				{ address:'ул Галиуллина, д.49', count: 100 },
				{ address:'ул Галиуллина, д.49/2', count: 70 },
				{ address:'ул Галиуллина, д.49/1', count: 100 },
				{ address:'ул Галиуллина, д.45', count: 112 },
				{ address:'ул Галиуллина, д.41/1', count: 60 },
				{ address:'ул Галиуллина, д.47/2', count: 70 },
				{ address:'ул Доменщиков, д.5/2', count: 119 },
				{ address:'ул Доменщиков, д.1', count: 88 },
				{ address:'ул Доменщиков, д.9', count: 86 },
				{ address:'ул 50-летия Магнитки, д.61', count: 162 },
				{ address:'ул 50-летия Магнитки, д.54', count: 131 },
				{ address:'ул Труда, д.21', count: 164 },
				{ address:'проезд Сиреневый, д.16/2', count: 60 },
				{ address:'пр-кт Карла Маркса, д.198/3', count: 100 },
				{ address:'ул Доменщиков, д.25', count: 88 },
				{ address:'ул Бориса Ручьева, д.17/1', count: 129 },
				{ address:'ул Доменщиков, д.13/1', count: 90 },
				{ address:'ул Доменщиков, д.26', count: 67 },
				{ address:'ул Мичурина, д.130', count: 98 },
				{ address:'ул Советская, д.195/1', count: 68 },
				{ address:'пр-кт Карла Маркса, д.176/1', count: 89 }]
			
			showRequestInfoAndTime(`Отчеты: запрос на данные по выполнению плана по месяцам (дома)`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			let firstDateOfYear = `${ (new Date()).getFullYear() }-01-01`
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) apiRes.status(400).send(connErr.detail)
				
				let results = Promise.all(plan.map(planItem => {
						const query = `select count(*), date_trunc('month', loaded) as month_loaded
								  from meters where customer_address like '%${ planItem.address }%'
								  and loaded > '${ firstDateOfYear }'
								  group by date_trunc('month', loaded)
								  order by date_trunc('month', loaded);`
						//console.log(query)
						return client.query(query).then(
							result => {
								//console.log(result.rows)
								return { address: planItem.address, data: result.rows }
							},
						);
					}
				));
				
				results.then(
					resolve => {
						done()
						apiRes.status(200).send(resolve)
					},
					error => {
						console.log(`Ошибка: ${ error }`);
						const message = error.message === undefined ? error.routine : error.message
						apiRes.status(400).send(message)
					}
				)
			})
			
		})
		
		app.get(`/api/${ module_name }/get-meter-from-repair-to-storage-report`, async (apiReq, apiRes) => {
			showRequestInfoAndTime(`Отчеты: запрос на количество счетчиков выданных из Ремонт(УИТ) на Склад`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			const oraConn = await getOraConnectionUit()
			//получение всех счетчиков которые находятся в данный момент на складе, с операцией выдача на программирование
			const query = `select m.serial_number
								from meter m, meter_log l
								where m.meter_location = 0
								and m.guid = l.meter_guid
								and l.old_location = 9
								and l.new_location = 0`
			
			oraConn.execute(query).then(
				resolve => {
					console.log(`Количество счетчиков для поиска ${ resolve.rows.length }`)
					//получение всех логов данного счетчика и выбор последних двух логов
					return Promise.all(resolve.rows.map(row =>
						oraConn
							.execute(`select * from meter_log
				                      where meter_serial_number = '${ row.SERIAL_NUMBER }'
				                      and rownum <= 2 order by id desc`)
							.then(result => result.rows)
					))
				}
			).then(
				resolve => {
					//если последние два лога операции выдачи на программирование и прием на склад
					//console.log(resolve)
					let resolveFiltered = resolve
						.filter(logs => logs[0].OPER_TYPE === 9 && logs[1].OPER_TYPE === 12)
						.map(logs => { return { serialNumber: logs[1].METER_SERIAL_NUMBER, date: logs[1].DATETIME }})
					
					console.log(`Количество найденных для отчета счетчиков ${ resolveFiltered.length }`)
					
					return Promise.all(resolveFiltered.map(meter => {
						const query = `select m.serial_number, m.meter_type, t.type_name from meter m, meter_type t
						                where m.serial_number = '${ meter.serialNumber }' and m.meter_type = t.type_index`
						//console.log(query)
						return oraConn.execute(query).then(
							result => {
								return { ...result.rows[0], date: meter.date }
							}
						)
					}))
				}
			).then(
				resolve => {
					oraConn.close()
					apiRes.status(200).send(resolve)
				}
			).catch(
				error => {
					oraConn.close()
					console.log(`Ошибка: ${ error }`)
					const message = error.message === undefined ? error.routine : error.message
					return apiRes.status(400).send(message)
				}
			)
		
			
		})
		
		app.get(`/api/${ module_name }/get-meter-not-loaded-in-pyramid`, async (apiReq, apiRes) => {
			
			showRequestInfoAndTime(`Отчеты: запрос на данные по не загруженным счетчикам в пирамиду`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			const query = `select m.serial_number, t.name, m.address,
			                m.phone, m.created from meters m, meter_type t
			                where in_pyramid = 0 and t.id = m.type order by t.name limit 1000`
			
			const oraConn = await getOraConnectionUit()
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) apiRes.status(400).send(connErr.detail)
				
				client.query(query).then(
					resolve => {
						if (!resolve.rows.length)
							return apiRes.status(400).send('Список не загруженных в пирамиду счетчиков пуст')
						
						const strSerNumbers = resolve.rows.map(row => `'${ row.serial_number}'`).join(',')
						//console.log(strSerNumbers.length)
						
						const query = `select serial_number from meter where serial_number in (${ strSerNumbers })
										and meter_location = 0 and current_owner = 12730`
						
						console.log(query)
						
						return { query: oraConn.execute(query), data: resolve.rows }
					})
				.then(
					async resolve => {
						const queryResult = await resolve.query
						console.log(queryResult)
						
						const storageMeters = queryResult.rows.flat()
						//console.log(storageMeters)
						
						const finalResult = resolve.data.map(row => {
							let inStorage = storageMeters.includes(parseInt(row.serial_number))
							return { ...row, inStorage }
						})
						done()
						apiRes.send(finalResult)
					}
				).catch(
					error => {
						done()
						console.log(`Ошибка: ${ error }`);
						const message = error.message === undefined ? error.routine : error.message
						apiRes.status(400).send(message)
					}
				)
			})
		})
		
		app.get(`/api/${ module_name }/get-non-active-meters-from-pyramid/:days`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) return
			
			let dayDepth = apiReq.params.days
			console.log(`Глубина дней = ${ dayDepth }`)
			
			showRequestInfoAndTime(`Отчеты: запрос на список не активных счетчиков в пирамиде`)
			
			if (dayDepth === '' || dayDepth === undefined) {
				return apiRes.status(400).send('Не задана глубина дат для отчета');
			}
			
			dayDepth = parseInt(dayDepth)
			const query = `select * from meters where in_pyramid = 1`
			
			const dateNow = new Date()
			const dayNow = dateNow.getDate()
			const monthNow = dateNow.getMonth() + 1
			const yearNow = dateNow.getFullYear()
			let dateFrom = new Date(yearNow, monthNow - 1, dayNow - dayDepth)
			dateFrom = `${ dateFrom.getFullYear() }-${ dateFrom.getMonth() + 1 }-${ dateFrom.getDate() }`
			const dateTill = `${ yearNow }-${ monthNow }-${ dayNow }`
			
			console.log(dateFrom)
			console.log(dateTill)
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) apiRes.status(400).send(connErr.detail)
				
				client
					.query(query)
					.then(
						async resolve => {
							const metersArray = resolve.rows
							const strSerialNumbers = metersArray.map(m => `'${ m.serial_number }'`).join(',')
							
							//запрос на полученные данные с АСД по серийному номеру счетчика в промежутке даты
							//или без даты вовсе
							const query = dayDepth !== 0
								? `select max(x.date_time) as last_date_time,
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
										: `SELECT ap.ЗавНомер as serial_number,  count(stack."АСД ДанНаСут".row_id)
			                                 FROM stack."АСД ДанНаСут", stack."АСД Приборы" ap, stack."АСД Каналы"
			                                 where ap.row_id = stack."АСД Каналы".Прибор
			                                 and stack."АСД Каналы".row_id = stack."АСД ДанНаСут".Канал
			                                 and ap.ЗавНомер in (${ strSerialNumbers })
			                             group by ap.ЗавНомер`
									//console.log(query)
							
							pgStekASDPool.connect((stekASDConnErr, stekASDClient, stekASDDone) => {
								if (stekASDConnErr) apiRes.status(400).send(stekASDConnErr.detail)
								
								stekASDClient
									.query(query)
									.then(
										resolve => {
											console.log(`Данные по показаниям счетчиков из АСД получены успешно`)
											
											console.log(resolve.rows.length)
											console.log(metersArray.length)
											
											const nonActiveMeters = metersArray.filter(m =>
												!resolve.rows.find(r => r.serial_number === m.serial_number)
											)
											
											const strSerialNumbers = nonActiveMeters.map(m => `'${ m.serial_number }'`).join(',')
											
											let queryLastData = `select serial_number, max(date_time) as last_date_time from (
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
													order by data."ДатаВремя" desc) x group by serial_number;`
											
											return { promise: stekASDClient.query(queryLastData), nonActiveMeters }
										})
									.then(
										async resolve => {
											console.log(`Данные по показаниям счетчиков из АСД получены успешно`)
											const queryResult = await resolve.promise
											
											let nonActiveMeters = resolve.nonActiveMeters
											
											nonActiveMeters = nonActiveMeters.map(meter => {
												const nonActiveMeter = queryResult.rows.filter(row => row.serial_number === meter.serial_number)
												if (nonActiveMeter.length) meter.last_date_time = nonActiveMeter[0].last_date_time
												return meter
											})
											done()
											stekASDDone()
											apiRes.send(nonActiveMeters)
										}
									)
							})
						})
					.catch(
						error => {
							done()
							const message = error.message === undefined ? error.routine : error.message
							return apiRes.status(400).send(message)
						}
					)
			})
		})
		
		app.get(`/api/${ module_name }/get-meter-count-by-address/:created`, (apiReq, apiRes) => {
			
			showRequestInfoAndTime(`Отчеты: запрос на данные сгруппированные по принадлежности и месяцу`)
			
			if (!checkAuth(apiReq, apiRes)) return
			
			let time = apiReq.params.created === '0' ? 'created' : 'loaded', queryParam = ''
			if (time === 'loaded') queryParam = 'in_pyramid = 1 and '
			
			console.log(`Выборка по времени = ${ time }`)
			let firstDateOfYear = `01-01-${ (new Date()).getFullYear() }`
			
			const query = `select count(*), address, date_trunc('month', ${ time }) "month"
			                            from meters where ${ queryParam }${ time } > '${ firstDateOfYear }'
                                        group by month, address order by month`
			//console.log(query)
			
			pgPool.connect((connErr, client, done) => {
				if (connErr) apiRes.status(400).send(connErr.detail)
				
				client.query(query).then(
					resolve => {
						if (!resolve.rows.length)
							return apiRes.status(400).send('Что то пошло не так при выполенении запроса...')
						
						const finalResult = resolve.rows.reduce((acc, cur) => {
							const dateMonth = new Date(cur.month).getMonth() + 1
							
							let address = acc.find(data => data.address === cur.address)
							if (!address) acc.push({address: cur.address, months: []})
							address = acc.find(data => data.address === cur.address)
							
							let month = address.months.find(data => data.month === dateMonth)
							if (!month) address.months.push({month: dateMonth, count: cur.count})
							
							return acc
						}, [])
						done()
						apiRes.status(200).send(finalResult)
					}
				).catch(
					error => {
						done()
						console.log(`Ошибка: ${ error }`)
						const message = error.message === undefined ? error.routine : error.message
						return apiRes.status(400).send(message)
					}
				)
				
			})
		})
	}
}