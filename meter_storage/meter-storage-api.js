const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getDateTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'meter-storage'
const acceptOrIssueOrRegApi = require('./modules/accept-or-issue-or-register-api')
const repairAndMaterialsApi = require('./modules/repair-and-materials')

module.exports = class MeterStorageApi {
	constructor(app) {
		new acceptOrIssueOrRegApi(app, module_name)
		new repairAndMaterialsApi(app, module_name)
		
		//Получение списка счетчиков постранично
		app.post(`/api/${ module_name }/meters`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { page, itemsPerPage, sortBy, sortDesc, role } = apiReq.body.options
			console.log(`Страница ${ page } - ${ itemsPerPage } сортировка ${ sortBy } ${ sortDesc }`)
			
			const roleOption = role === 'repairer' ? ` where meter_location = 1 ` : ''
			const query = "select id, meter_type, serial_number, accuracy_class, passport_number, condition, " +
							"calibration_interval, calibration_date, meter_location, current_owner, property, guid " +
							`from meter ${ roleOption } order by id ${ sortDesc[0] ? 'desc' : '' }`
			
			const client = await pgPool.connect()
			try {
				const { rows } = await client.query(query)
				const total = rows.length
				let data = rows.slice((page - 1) * itemsPerPage, page * itemsPerPage)
				
				if (role === 'repairer') {
					data = await Promise.all(data.map(async (row) => {
						const queryLog =
							"select meter_log.update_field from meter_log " +
							"where meter_log.id = ( select max(ml.id)  " +
							"from meter m, meter_log ml " +
							"where m.meter_location = 1 " +
							`and m.id = ${ row.id } ` +
							"and m.guid = ml.meter_guid " +
							"and ml.oper_type = 1)"
						
						const { rows } = await client.query(queryLog)
						const [ lastUpdateField ] = rows
						const updateField = lastUpdateField.update_field
						let color = 0
						if (updateField) {
							color = updateField.includes('Статус ремонта:')
								? 2
								: updateField.includes('Используемые материалы:')
									? 1
									: 0
						}
						
						return { ...row, repairColor: color }
					}))
				}
				
				apiRes.send({ rows: data, total })
			} catch (e) {
				console.log(e)
				apiRes.status(400).send(e.detail)
			} finally {
				client.release()
			}
		})
		
		app.get(`/api/${ module_name }/meter-types`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes))
				return
			
			const query = `select * from meter_storage_type order by type_name`
			
			executePGIQuery(query, apiRes)
		})
		
		app.post(`/api/${ module_name }/filter`, async (apiReq, apiRes) => {
			const { error } = _validateFilters(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { serialNumber, types, locations, owners } = apiReq.body.filters
			const { page, itemsPerPage, sortBy, sortDesc, role } = apiReq.body.options
			console.log(`Страница ${ page } - ${ itemsPerPage } сортировка ${ sortBy } ${ sortDesc }`)
			
			let filters = []
			let desc = ''
			
			if (sortDesc && sortDesc[0]) {
				desc = 'desc'
			}
			
			
			if (serialNumber) {
				const serNumber = serialNumber.trim()
				const serialNumbers = serNumber.split(' ').map((serialNumber) => `'${ serialNumber }'`)
				if (serialNumbers.length > 1) {
					filters.push(`serial_number in (${ serialNumbers.join(',') })`)
				} else {
					filters.push(`serial_number like '%${ serNumber }%'`)
				}
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
			
			let roleOption = ''
			if (role === 'repairer') {
				roleOption = ` ${ queryBody ? 'and' : '' } meter_location = 1 `
			}
			
			const query = `select * from meter where ${ queryBody } ${ roleOption } order by id ${ desc }`
			const client = await pgPool.connect()
			try {
				let { rows } = await client.query(query)
				const total = rows.length
				
				if (page && itemsPerPage) {
					if (total > itemsPerPage) {
						rows = rows.slice((page - 1) * itemsPerPage, page * itemsPerPage)
					}
				}
				return apiRes.send({ rows, total })
			} catch ({ message }) {
				apiRes.status(400).send(message)
			} finally {
				client.release()
			}
		})
		
		app.get(`/api/${ module_name }/storage-employees`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			executePGIQuery(`select staff_id, name, card from employees`, apiRes)
		})
		
		app.get(`/api/${ module_name }/logs/:guid`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const guid = apiReq.params.guid
			if (!guid) {
				return apiRes.status(400).send('GUID счетчика отсутствует')
			}
			
			const query = `select * from meter_log where meter_guid = '${ guid }' order by id desc, datetime desc`
			executePGIQuery(query, apiRes)
		})
		
		app.get(`/api/${ module_name }/parse-options`, (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			executePGIQuery(`select id, barcode_thrue_index as parse_option from meter_mnf`, apiRes)
		})
		
		app.post(`/api/${ module_name }/edit-log-comment`, async (apiReq, apiRes) => {
			const { error } = _validateEditComment(apiReq.body)
			if (error) {
				return apiRes.status(400).send(error.details[0].message)
			}
			
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const { comment, logId } = apiReq.body
			const query = `update meter_log set comment_field = '${ comment }' where id = ${ logId }`
			executePGIQuery(query, apiRes)
		})
	}
}

function _validateEditComment(meter) {
	const schema = {
		logId: joi.number().required(),
		comment: joi.string().empty('').allow(null)
	}
	return joi.validate(meter, schema)
}

function _validateFilters(meter) {
	const schema = {
		filters: joi.object().required(),
		options: joi.object(),
	}
	return joi.validate(meter, schema);
}