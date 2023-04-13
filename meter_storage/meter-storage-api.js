const { getOraConnectionUit } = require('../database/oracle/oracle-db-connection.js')
const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getDateTime, showRequestInfoAndTime, joi, executePGIQuery, executeOraQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'meter-storage'
const acceptOrIssueOrRegApi = require('./modules/accept-or-issue-or-register-api')
const repairAndMaterialsApi = require('./modules/repair-and-materials')

module.exports = class MeterStorageApi {
	constructor(app) {
		new acceptOrIssueOrRegApi(app, module_name)
		new repairAndMaterialsApi(app, module_name)
		
		//Получение списка счетчиков
		app.get(`/api/${ module_name }/meters`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на информацию о счетчиках склада')
			
			const query = `select
								id,
								meter_type,
								serial_number,
								accuracy_class,
								passport_number,
								condition,
								calibration_date,
								calibration_interval,
								meter_location,
								current_owner,
								property,
								guid
							from meter where rownum <= 100`
			
			executeOraQuery(query, apiRes)
		})
		
		//Получение списка счетчиков постранично
		app.post(`/api/${ module_name }/meters`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			const { page, itemsPerPage, sortBy, sortDesc } = apiReq.body.options
			
			console.log(`Страница ${ page } - ${ itemsPerPage } сортировка ${ sortBy } ${ sortDesc }`)
			showRequestInfoAndTime('Склад счетчиков: запрос на информацию о счетчиках склада порциями')
			
			const query = `select
								id,
								meter_type,
								serial_number,
								accuracy_class,
								passport_number,
								condition,
								calibration_date,
								calibration_interval,
								meter_location,
								current_owner,
								property,
								guid
							from meter order by id ${ sortDesc[0] ? 'desc' : '' }`
			
			getOraConnectionUit().then(
				oraConn => {
					oraConn.execute(query).then(
						result => {
							oraConn.close()
							const total = result.rows.length
							const rows = result.rows.slice((page - 1) * itemsPerPage, page * itemsPerPage)
							
							apiRes.send({ rows, total })
						},
						error => {
							oraConn.close()
							console.log(`Запрос (${ query }). Ошибка: ${ error }`)
							apiRes.status(400).send(error.detail)
						}
					)
				}
			)
		})
		
		app.get(`/api/${ module_name }/meter-types`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на типы счетчиков')
			
			const query = `select * from meter_type order by type_name`
			
			executeOraQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/filter`, async (apiReq, apiRes) => {
			const { error } = _validateFilters(apiReq.body)
			if (error) return apiRes.status(400).send(error.details[0].message)
			
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime(`Склад счетчиков: запрос на фильтрацию`)
			//console.log(apiReq.body)
			
			const { serialNumber, types, locations, owners } = apiReq.body.filters
			const { page, itemsPerPage, sortBy, sortDesc } = apiReq.body.options
			//console.log(`Страница ${ page } - ${ itemsPerPage } сортировка ${ sortBy } ${ sortDesc }`)
			
			let filters = []
			let desc = ''
			
			if (sortDesc && sortDesc[0]) {
				desc = 'desc'
			}
			
			if (serialNumber) {
				filters.push(`serial_number like '%${ serialNumber }%'`)
			}
			
			if (types && types.length) {
				filters.push(`meter_type in (${ types })`)
			}
			
			if (locations && locations.length) {
				filters.push(`meter_location in (${ locations })`)
			}
			
			if (owners && owners.length) {
				filters.push(`current_owner in (${ owners })`)
			}
			
			if (!filters.length) {
				return apiRes.status(400).send('Ошибка при фильтрациии')
			}
			
			let queryBody = filters.reduce((sum, cur, i) => {
				if (i > 0) {
					sum += ' and '
				}
				return sum + cur
			}, '')
			
			const query = `select * from meter  where ${ queryBody } order by id ${ desc }`
			console.log(query)
			try {
				const conn = await getOraConnectionUit()
				const queryRes = await conn.execute(query);
				conn.close()
				let rows = queryRes.rows
				const total = rows.length
				console.log(total)
				if (page && itemsPerPage) {
					if (total > itemsPerPage) {
						rows = rows.slice((page - 1) * itemsPerPage, page * itemsPerPage)
					}
				}
				return apiRes.send({ rows, total })
			} catch ({ message }) {
				apiRes.status(400).send(message)
			}
		})
		
		app.get(`/api/${ module_name }/employees`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на список сотрудников')
			
			const query = `select * from meter_employees`
			
			executeOraQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/storage-employees`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на список внутренних сотрудников')
			
			const query = `select staff_id, name, card from employees`
			
			executePGIQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/logs/:GUID`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			const GUID = apiReq.params.GUID
			if (!GUID)
				return apiRes.status(400).send('GUID счетчика отсутствует')
			
			showRequestInfoAndTime('Склад счетчиков: запрос на счетчик по guid')
			
			const query = `select * from meter_log where meter_guid = '${ GUID }' order by id desc, datetime desc`
			
			executeOraQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/parse-options`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			showRequestInfoAndTime('Склад счетчиков: запрос на опции парсинга')
			
			const query = `select id, barcode_thrue_index as parse_option from meter_mnf`
			
			executeOraQuery(query, apiRes)
		})
	}
}

function _validateFilters(meter) {
	const schema = {
		filters: joi.object().required(),
		options: joi.object(),
	}
	return joi.validate(meter, schema);
}