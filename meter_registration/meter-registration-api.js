const { pgPool } = require("../database/postgres/postgres-db-connection"),
{ getDateTime, showRequestInfoAndTime, joi, executePGIQuery } = require('../utils'),
{ checkAuth } = require('../login/login-api'),
module_name = 'meter-registration',
actualizeFromRTCApi = require('./modules/actualize-from-rtc-api'),
actualizeFromStekApi = require('./modules/actualize-from-stek-api'),
refreshFromStekApi = require('./modules/refresh-from-stek-api')

module.exports = class MeterRegistrationApi {
    constructor(app) {
        new actualizeFromRTCApi(app, module_name)
        new actualizeFromStekApi(app, module_name)
        new refreshFromStekApi(app, module_name)
        
        //Получение списка счетчиков
        app.post(`/api/${ module_name }/meters`, (apiReq, apiRes) => {
            const { error } = _validateMeters(apiReq.body);
            if (error) return apiRes.status(400).send(error.details[0].message);

            if (!checkAuth(apiReq, apiRes)) {
                return
            }
            showRequestInfoAndTime('Регистрация счетчиков: запрос на информацию о счетчиках')
            
            const inPyramid = apiReq.body.inPyramid === true ? 1 : 0
            const query = `select * from meters where in_pyramid = ${ inPyramid }`
    
            executePGIQuery(query, apiRes)
        })

        //Добавление счетчика
        app.post(`/api/${ module_name }/meter`, (apiReq, apiRes) => {
            const { error } = _validateMeter(apiReq.body)
            if (error) {
                return apiRes.status(400).send(error.details[0].message)
            }
    
            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) {
                return
            }

            showRequestInfoAndTime(`Регистрация счетчиков: запрос на добавление счетчика с данными:
                                                 серийный номер - ${ apiReq.body.serialNumber },
                                                 тип - ${ apiReq.body.type },
                                                 фазность - ${ apiReq.body.phase },
                                                 icc - ${ apiReq.body.icc },
                                                 port - ${ apiReq.body.port },
                                                 address - ${ apiReq.body.address },
                                                 parentId - ${ apiReq.body.parentId },
                                                 contact - ${ apiReq.body.contact },
                                                 gateway - ${ apiReq.body.gateway }`)
            
            //console.log(apiReq.body)
            const port = !apiReq.body.port ? null : apiReq.body.port
            const icc = !apiReq.body.icc ? null : apiReq.body.icc
            const contact = !apiReq.body.contact ? null : apiReq.body.contact
            const parentId = !apiReq.body.parentId ? null : apiReq.body.parentId
            const gateway = !apiReq.body.gateway ? null : apiReq.body.gateway
            const time = getDateTime()
            const smsStatus = [ 5, 6, 7, 18, 20, 21, 22 ].includes(apiReq.body.type) ? 7 : 0 //МИРы не требуют смс

            const query = `insert into meters (
                                            serial_number,
                                            type,
                                            phase,
                                            icc,
                                            port,
                                            address,
                                            contact,
                                            sms_status,
                                            created,
                                            ip_address,
                                            parent_id,
                                            gateway,
                                            author_acc_id)
                                        values (
                                            '${ apiReq.body.serialNumber }',
                                            ${ apiReq.body.type },
                                            ${ apiReq.body.phase },
                                            ${ icc },
                                            ${ port },
                                            '${ apiReq.body.address }',
                                            ${ contact },
                                            ${ smsStatus },
                                            '${ time }',
                                            '${ apiReq.body.ipAddress }',
                                            ${ parentId },
                                            ${ gateway },
                                            ${ authResult.id }
                                        ) returning *`
    
            executePGIQuery(query, apiRes)
        })

        //Удаление счетчика с логированием
        app.delete(`/api/${ module_name }/meters/:id`, async (apiReq, apiRes) => {
            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) {
                return
            }
            
            const meterId = apiReq.params.id;
            showRequestInfoAndTime(`Регистрация счетчиков: запрос на удаление счетчика с id = ${ meterId }`)
            
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client
                    .query(`select * from meters`)
                    .then(
                        queryResult => {
                            const meters = queryResult.rows
                            const meter = meters.find(m => m.id === parseInt(meterId));
                            if (!meter) throw new Error('Счетчика с данным id не найдено')
                
                            const query = `delete from meters where id = ${meterId} returning id`
                            return { promise: client.query(query), meter: meter }
                        })
                    .then(
                        async result => {
                            const queryResult = await result.promise
                            const queryLog =
                                `insert into meter_reg_log
                                    (
                                        log_type,
                                        state_before,
                                        state_after,
                                        author_acc_id,
                                        meter_id,
                                        created
                                    )
                            values (
                                        1,
                                        '${ JSON.stringify(result.meter) }',
                                        '{}',
                                        ${ authResult.id },
                                        ${ meterId },
                                        '${ getDateTime() }'
                                    )
                            returning id`
                
                            return { promise : client.query(queryLog), queryMeterResult: queryResult }
                        })
                    .then(
                        async result => {
                            const deletedMeterId = result.queryMeterResult.rows[0].id
                            console.log(`Счетчик ${deletedMeterId} удален успешно\n`)
                
                            const queryResult = await result.promise
                            console.log(`Лог удаления счетчика ${meterId} создан успешно (${queryResult.rows[0].id})\n`)
                            done()
                            return apiRes.status(200).send([{ id: deletedMeterId }])
                        })
                    .catch(
                        error => {
                            const message = error.message === undefined ? error.routine : error.message
                            done()
                            return apiRes.status(400).send(message)
                        }
                    )
            })
        })

        //Редактирование счетчика с логированием
        app.put(`/api/${ module_name }/meters/:id`, async (apiReq, apiRes) => {
            const { error } = _validateMeter(apiReq.body);
            if (error) return apiRes.status(400).send(error.details[0].message);
            
            const meterAfter = apiReq.body
            const meterId = apiReq.params.id
            const port = !meterAfter.port ? null : meterAfter.port
            const contact = !meterAfter.contact ? null : meterAfter.contact
            const serialNumber = meterAfter.serialNumber
            const type = meterAfter.type
            const phase = meterAfter.phase
            const icc = !meterAfter.icc ? null : meterAfter.icc
            const address = meterAfter.address
            const ipAddress = meterAfter.ipAddress
            const parentId = !apiReq.body.parentId ? null : apiReq.body.parentId
            const gateway = !apiReq.body.gateway ? null : apiReq.body.gateway
            //console.log(meterAfter)
            
            showRequestInfoAndTime(`Регистрация счетчиков: запрос на редактирование счетчика с id = ${ meterId }`)
            
            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return

            const query = `update meters set (
                                            serial_number,
                                            type,
                                            phase,
                                            icc,
                                            port,
                                            address,
                                            ip_address,
                                            parent_id,
                                            gateway,
                                            contact)
                                        = (                                                                            
                                            '${ serialNumber }',
                                            ${ type },
                                            ${ phase },
                                            ${ icc },
                                            ${ port },
                                            '${ address }',
                                            '${ ipAddress }',
                                            ${ parentId },
                                            ${ gateway },
                                            ${ contact }
                                        ) where id = ${meterId} returning *`
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client
                    .query(`select * from meters`)
                    .then(
                        queryResult => {
                            const meters = queryResult.rows
                            const meter = meters.find(m => m.id === parseInt(meterId));
                            if (!meter) throw new Error('Счетчика с данным id не найдено')
    
                            return { promise: client.query(query), meter }
                        })
                    .then(
                        async result => {
                            const queryResult = await result.promise
                            console.log(queryResult.rows[0])
                            const queryLog =
                                `insert into meter_reg_log
                                     (
                                        log_type,
                                        state_before,
                                        state_after,
                                        author_acc_id,
                                        meter_id,
                                        created
                                    )
                                values
                                     (
                                        2,
                                        '${ JSON.stringify(result.meter) }',
                                        '${ JSON.stringify(queryResult.rows[0]) }',
                                        ${ authResult.id },
                                        ${ meterId },
                                        '${ getDateTime() }'
                                     ) returning id`
    
                            //console.log(queryLog)
                            return { promise : client.query(queryLog), queryMeterResult: queryResult }
                        })
                    .then(
                        async result => {
                            const editedMeter = result.queryMeterResult.rows[0]
                            console.log(`Счетчик ${editedMeter.id} отредактирован успешно\n`)
    
                            const queryResult = await result.promise
                            console.log(`Лог редактирования счетчика ${meterId} создан успешно (${queryResult.rows[0].id})\n`)
    
                            return apiRes.status(200).send([ editedMeter ])
                        })
                    .catch(
                        error => {
                            const message = error.message === undefined ? error.routine : error.message
                            return apiRes.status(400).send(message)
                        }
                    )
            })
        })

        //Сохранение данных после загрузки в Пирамиду
        app.post(`/api/${ module_name }/save-meter-after-load-in-stek`, (apiReq, apiRes) => {
            const { error } = _validatePyramidLoad(apiReq.body)
            if (error) return apiRes.status(400).send(error.details[0].message)

            const meterArray = apiReq.body.meterArray
            showRequestInfoAndTime(`Получен запрос на сохранение данных после загрузки в СТЭК`)
        
            if (!checkAuth(apiReq, apiRes)) return

            const query = `update meters set in_pyramid = 1, loaded = '${ getDateTime() }'
                                                where id in (${ meterArray.toString() }) returning *`
            
            executePGIQuery(query, apiRes)
        })

        //Утилизация счетчика
        app.post(`/api/${ module_name }/mark-meter/:id`, (apiReq, apiRes) => {
            const meterId = apiReq.params.id
            const { error } = _validateMarkMeter(apiReq.body)
            if (error) return apiRes.status(400).send(error.details[0].message)

            const reason = apiReq.body.reason
            const data = apiReq.body.data
            const comment = apiReq.body.comment

            showRequestInfoAndTime(`Получен запрос на утилзацию счетчика: id = ${ meterId }`)
            
            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return

            const query = `insert into meters_broken (
                                                        meter_id,
                                                        reason,
                                                        created,
                                                        comment,
                                                        data,
                                                        acc_id
                                                     )
                                                     values
                                                     (
                                                        ${ meterId },
                                                        ${ reason },
                                                        '${ getDateTime() }',
                                                        '${ comment }',
                                                        '${ data }',
                                                        ${ authResult.id })
                                                     returning *`
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client
                    .query(query)
                    .then(
                        queryResult => {
                            const meterId = queryResult.rows[0].meter_id
    
                            const query = `update meters set (
                                                address,
                                                sms_id,
                                                sms_status,
                                                customer,
                                                customer_email,
                                                customer_phone,
                                                customer_type,
                                                customer_address,
                                                in_pyramid,
                                                loaded,
                                                personal_account)
                                            = (
                                                'СНЯТ',
                                                null,
                                                1,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                0,
                                                null,
                                                null
                                            ) where id = ${ meterId } returning *`
    
                            return { promise: client.query(query), meterId: meterId }
                        }
                    ).then(
                    async data => {
                        console.log(`Счетчик ${ data.meterId } утилизирован успешно\n`)
                        const queryResult = await data.promise
                        done()
                        return apiRes.status(200).send(queryResult.rows)
                    }).catch(
                        error => {
                            done()
                            const message = error.message === undefined ? error.routine : error.message
                            return apiRes.status(400).send(message)
                        }
                    )
            })
        })

        //Получение списка утилизированных счетчиков
        app.get(`/api/${ module_name }/broken-meters`, (apiReq, apiRes) => {
            showRequestInfoAndTime('Регистрация счетчиков: запрос на информацию о списанных счетчиках')

            if (!checkAuth(apiReq, apiRes)) return

            const query = `select * from meters_broken`
            const queryAcc = `select id, full_name from accounts`
            
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
        
                const promises = [ client.query(query), client.query(queryAcc) ], result = {}

                Promise.all(promises).then(
                    responses => {
                        responses.forEach(response => {
                            if (response.rows.length) {
                                response.fields && response.fields.some(field => field.name === 'full_name')
                                    ? result.accounts = response.rows
                                    : result.meters = response.rows
                            }
                        })
                        done()
                        return apiRes.status(200).send(result)
                    },
                    error => {
                        done()
                        console.log(`Ошибка: ${ error }`);
                        const message = error.message === undefined ? error.routine : error.message
                        return apiRes.status(400).send(message)
                    }
                )
            })
        })

        //Установка признака загрузки в пирамиду
        app.post(`/api/${ module_name }/remove-meter-pyramid-load-value`, (apiReq, apiRes) => {
            const { error } = _validateRemoveMeterPyramidLoadValue(apiReq.body)
            if (error) return apiRes.status(400).send(error.details[0].message)

            showRequestInfoAndTime(`Регистрация счетчиков: запрос на удаление признака загрузки в пирамиду`)

            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return
            
            const meter = apiReq.body.meter
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client.query(`update meters set in_pyramid = 0, loaded = null where id = ${ meter.id } returning *`)
                    .then(
                        queryResult => {
                            const updMeter = queryResult.rows[0]
                            const queryLog =
                                `insert into meter_reg_log(
                                                            log_type,
                                                            state_before,
                                                            state_after,
                                                            author_acc_id,
                                                            meter_id,
                                                            created
                                                         ) values (
                                                             3,
                                                             '${ JSON.stringify(meter) }',
                                                             '${ JSON.stringify(updMeter) }',
                                                             ${ authResult.id },
                                                             ${ meter.id },
                                                             '${ getDateTime() }')
                                                         returning id`
    
                            return { promise : client.query(queryLog), queryMeterResult: queryResult }
                        })
                    .then(
                    async result => {
                        const updateMeterId = result.queryMeterResult.rows[0].id
                        console.log(`Счетчик ${updateMeterId} изменен успешно\n`)
    
                        const queryResult = await result.promise
                        console.log(`Лог изменения признака загрузки в пирамиду счетчика ${ meter.id } создан успешно (${ queryResult.rows[0].id })\n`)
                        
                        done()
                        apiRes.status(200).send(result.queryMeterResult.rows)
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
        
        //Удаление признака загрузки в пирамиду
        app.post(`/api/${ module_name }/add-meter-pyramid-load-value`, (apiReq, apiRes) => {
            const { error } = _validateRemoveMeterPyramidLoadValue(apiReq.body)
            if (error) return apiRes.status(400).send(error.details[0].message)

            showRequestInfoAndTime(`Регистрация счетчиков: запрос на установку признака загрузки в пирамиду`)

            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return
            
            const meter = apiReq.body.meter
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client.query(`update meters set in_pyramid = 1, loaded = '${ getDateTime() }' where id = ${ meter.id } returning *`)
                    .then(
                        queryResult => {
                            const updMeter = queryResult.rows[0]
                            const queryLog =
                                `insert into meter_reg_log(
                                                            log_type,
                                                            state_before,
                                                            state_after,
                                                            author_acc_id,
                                                            meter_id,
                                                            created
                                                         ) values (
                                                             3,
                                                             '${ JSON.stringify(meter) }',
                                                             '${ JSON.stringify(updMeter) }',
                                                             ${ authResult.id },
                                                             ${ meter.id },
                                                             '${ getDateTime() }')
                                                         returning id`
    
                            return { promise : client.query(queryLog), queryMeterResult: queryResult }
                        })
                    .then(
                    async result => {
                        const updateMeterId = result.queryMeterResult.rows[0].id
                        console.log(`Счетчик ${ updateMeterId } изменен успешно\n`)
    
                        const queryResult = await result.promise
                        console.log(`Лог изменения признака загрузки в пирамиду счетчика ${ meter.id } создан успешно (${ queryResult.rows[0].id })\n`)
                        
                        done()
                        apiRes.status(200).send(result.queryMeterResult.rows)
                    }).catch(
                    error => {
                        done()
                        const message = error.message === undefined ? error.routine : error.message
                        return apiRes.status(400).send(message)
                    }
                )
            })
        })
    
        //Получение родительских счетчиков
        app.get(`/api/${ module_name }/get-parent-mirc04-meters`, (apiReq, apiRes) => {
            showRequestInfoAndTime('Регистрация счетчиков: запрос на список родительских счетчиков МИР С04')
        
            if (!checkAuth(apiReq, apiRes)) {
                return
            }
        
            const query = `select * from meters where type = 22`
            
            executePGIQuery(query, apiRes)
        })
    }
}

function _validateMeters(meter) {
    const schema = {
        inPyramid: joi.boolean().required(),
    }
    return joi.validate(meter, schema);
}

function _validateMeter(meter) {
    const schema = {
        serialNumber: joi.string().min(8).required(),
        type: joi.number().required(),
        phase: joi.number().required(),
        icc: joi.string().allow(null).empty(''),
        port: joi.number().required(),
        address: joi.string().allow(null).empty(''),
        contact: joi.number().allow(null).empty(''),
        ipAddress: joi.number().required(),
        parentId: joi.number().allow(null).empty(''),
        gateway: joi.number().allow(null).empty(''),
    }
    return joi.validate(meter, schema);
}

function _validatePyramidLoad(meter) {
    const schema = {
        meterArray: joi.array().required(),
    }
    return joi.validate(meter, schema);
}

function _validateMarkMeter(meter) {
    const schema = {
        reason: joi.number().required(),
        comment: joi.string().empty(''),
        data: joi.string().required(),
    }
    return joi.validate(meter, schema);
}

function _validateRemoveMeterPyramidLoadValue(meter) {
    const schema = {
        meter: joi.object().required()
    }
    return joi.validate(meter, schema);
}
