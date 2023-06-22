const { pgPool } = require("../database/postgres/postgres-db-connection")
const { getCurrentDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils')
const { checkAuth } = require('../login/login-api')
const module_name = 'common'

module.exports = class CommonApi {
	constructor(app) {
		app.get(`/api/${ module_name }/accounts`, async (apiReq, apiRes) => {
			await simpleExecute('select id, full_name from accounts order by id', apiReq, apiRes)
		})
		
		app.get(`/api/${ module_name }/assignment-event-types`, async (apiReq, apiRes) => {
			await simpleExecute('select * from assignment_event_type', apiReq, apiRes)
		})
		
		app.get(`/api/${ module_name }/assignment-close-reason-types`, async (apiReq, apiRes) => {
			await simpleExecute('select * from assignment_close_reason_type', apiReq, apiRes)
		})
		
		app.get(`/api/${ module_name }/dictionaries`, async (apiReq, apiRes) => {
			if (!checkAuth(apiReq, apiRes)) {
				return
			}
			
			const dictionariesQueryArray = [
				{ query: 'select * from assignment_close_reason_type order by id', title: 'assignmentCloseReasonTypes' },
				{ query: 'select * from assignment_event_type order by id', title: 'assignmentEventTypes' },
				{ query: 'select id, full_name from accounts order by id', title: 'accounts' },
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
