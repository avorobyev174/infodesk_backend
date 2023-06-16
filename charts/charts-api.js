const { pgPool } = require("../database/postgres/postgres-db-connection"),
oracledb = require('oracledb'),
{ pgStekASDPool } = require("../database/postgres/postgres-stek-asd-db-connection"),
{ showRequestInfoAndTime, joi, executePGIQuery } = require('../utils'),
{ checkAuth } = require('../login/login-api'),
module_name = 'charts'

module.exports = class ChartApi {
    constructor(app) {
        //график (число зарегистрированных/добавленных в пирамиду)
        app.get(`/api/${module_name}/meter-registration-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по количеству зарегистрированных счетчиков`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*), date_trunc('day', created) "day"
                                                        from meter_reg group by day order by day`
    
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-in-pyramid-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по количеству счетчиков добавленных в пирамиду`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*), date_trunc('day', loaded) "day"
                                                    from meter_reg where in_pyramid = 1 group by day order by day`
    
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-registration-types-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по разделению счетчиков по типу`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*), type from meter_reg group by type order by count desc`
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-in-pyramid-houses-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по разделению счетчиков по улицам`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select * from meter_reg where personal_account is not null and loaded is not null and in_pyramid = 1`
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-in-pyramid-customer-types-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по разделению счетчиков по типу лица`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*), customer_type from meter_reg where in_pyramid = 1 group by customer_type`
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-in-pyramid-count-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по числу загруженных в пирамиду`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*) from meter_reg group by in_pyramid`
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-not-in-pyramid-types-chart`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Графики: запрос по числу не загруженных в пирамиду`)

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select count(*), type from meter_reg where in_pyramid = 0 group by type order by count desc`
            executePGIQuery(query, apiRes)
        })

        app.get(`/api/${module_name}/meter-active-in-pyramid-chart`, (apiReq, apiRes) => {
            if (!checkAuth(apiReq, apiRes)) return

            showRequestInfoAndTime(`Графики: запрос по числу опрашиваемых счетчиков в пирамиде`)

            const query = `select * from meter_reg where in_pyramid = 1`
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
                
                client.query(query)
                    .then(
                        result => {
                            const metersArray = result.rows
                            const strSerialNumbers = metersArray.map(m => `'${ m.serial_number }'`).join(',')
                            
                            const query = `SELECT ap.ЗавНомер as serial_number, count(stack."АСД ДанНаСут".row_id)
                                     FROM stack."АСД ДанНаСут", stack."АСД Приборы" ap, stack."АСД Каналы"
                                     where ap.row_id = stack."АСД Каналы".Прибор
                                     and stack."АСД Каналы".row_id = stack."АСД ДанНаСут".Канал
                                     and ap.ЗавНомер in (${ strSerialNumbers })
                                 group by ap.ЗавНомер`
                            
                            pgStekASDPool.connect((stekASDConnErr, stekASDClient, stekASDDone) => {
                                if (stekASDConnErr) apiRes.status(400).send(stekASDConnErr.detail)
    
                                stekASDClient
                                    .query(query)
                                    .then(
                                        resolve => {
                                            //console.log(`Показания по счетчикам получены успешно\n`)
                                            done()
                                            stekASDDone()
                                            apiRes.status(200).send({ withData: resolve.rows.length, total: metersArray.length })
                                        })
                                    .catch(
                                        error => {
                                            stekASDDone()
                                            console.log(`Запрос (${ query }). Ошибка: ${ error }`)
                                            apiRes.status(400).send(error.routine)
                                        }
                                    )
                            })
                        })
                    .catch(
                        error => {
                            done()
                            const message = error.message === undefined ? error.routine : error.message
                            apiRes.status(400).send(message)
                        }
                    )
            })
        })
    }
}