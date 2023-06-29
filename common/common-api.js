const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getCurrentDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'common'

module.exports = class CommonApi {
	constructor(app) {
		app.get(`/api/${ module_name }/dictionaries`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const dictionariesQueryArray = [
				{ query: 'select * from assignment_close_reason_type order by id', title: 'assignmentCloseReasonTypes' },
				{ query: 'select * from assignment_event_type order by id', title: 'assignmentEventTypes' },
				{ query: 'select id, full_name as name, photo_url_sm as photo from accounts order by id', title: 'accounts' },
				{ query: 'select id as value, type_name as title from meter_storage_type where is_prog = 1 order by type_name', title: 'meterTypes' },
				{ query: 'select id as value, title from meter_ip_address order by id', title: 'ipAddresses' },
				{ query: 'select id as value, title from meter_sim_status order by id', title: 'simStatuses' },
				{ query: 'select id as value, title from assignment_status order by id', title: 'assignmentStatuses' },
			]
			const client = await pgPool.connect()
			try {
				const dictionaries = await Promise.all(dictionariesQueryArray.map(async ({ query, title }) => {
					const { rows } = await client.query(query)
					return { title, value: rows }
				}))
				apiRes.send(dictionaries)
			} catch (e) {
				apiRes.status(400).send(e.message)
			} finally {
				client.release()
			}
		})
	}
}

async function simpleExecute(query, apiReq, apiRes) {
	if (checkAuth(apiReq, apiRes)) {
		await executePGIQuery(query, apiRes)
	}
}
