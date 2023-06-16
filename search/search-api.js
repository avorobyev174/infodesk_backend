const { showRequestInfoAndTime } = require('../utils'),
{ pgStekASDPool } = require("../database/postgres/postgres-stek-asd-db-connection"),
{ pgPool } = require("../database/postgres/postgres-db-connection"),
{ checkAuth } = require('../login/login-api'),
module_name = 'search'

module.exports = class SearchApi {
    constructor(app) {
        app.get(`/api/${module_name}/get-meter-by-serial-number/:serialNumber`, (apiReq, apiRes) => {
            if (!checkAuth(apiReq, apiRes)) return

            const serialNumber = apiReq.params.serialNumber;
            showRequestInfoAndTime(`Получен запрос на данные по счетчику ${ serialNumber } в модуле поиска`)

            const query = `select serial_number, type, personal_account, customer_address, customer_type 
                                                                    from meter_reg where serial_number = '${ serialNumber }'`

            const queryAsd = `select data."ДатаВремя" as date_time, ac."ТипКанала" as channel_type, data."Значение" as value
                                from stack."АСД Приборы" ap, stack."АСД Каналы" ac, stack."АСД ДанНаСут" data
                                where ap.ЗавНомер in ('${ serialNumber }')
                                and ap.row_id = ac.Прибор
                                and ac."ТипКанала" in (1000,1001,1002)
                                and ac.row_id = data.Канал
                                order by data."ДатаВремя" desc
                                limit 3`;
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                pgStekASDPool.connect((stekASDconnErr, stekASDclient, stekASDdone) => {
                    if (stekASDconnErr) apiRes.status(400).send(stekASDconnErr.detail)
    
                    const promises = [ client.query(query), stekASDclient.query(queryAsd) ], resultArr = []
    
                    Promise.all(promises).then(
                        responses => {
                            responses.forEach(response => {
                                //console.log(response.rows)
                                resultArr.push(response.rows.length ? response.rows : [])
                            })
                            done()
                            stekASDdone()
                            apiRes.status(200).send(resultArr)
                        },
                        error => {
                            done()
                            stekASDdone()
                            console.log(`Ошибка: ${ error }`);
                            const message = error.message === undefined ? error.routine : error.message
                            apiRes.status(400).send(message)
                        }
                    )
                })
            })
        })
    }
}