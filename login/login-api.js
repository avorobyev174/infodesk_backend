const { pgPool } = require("../database/postgres/postgres-db-connection"),
{ showRequestInfoAndTime, jwt, authKey, roleKey, joi, tokenExp } = require('../utils')

module.exports = class LoginApi {
    constructor(app) {
        //Логин
        app.post('/login', async (apiReq, apiRes) => {
            const { error } = _validateLogin(apiReq.body);
            if (error) return apiRes.status(400).send(error.details[0].message)

            const name = apiReq.body.name
            const password = apiReq.body.password
    
            showRequestInfoAndTime(`Получен запрос на авторизацию ${ name } ${ password }`)
           
            try {
                const client = await pgPool.connect()
                console.log('pool connect')
                const accounts = await client.query(`select * from accounts`)
                console.log(accounts.rows)
                console.log(accounts.rows.length)
                if (!(accounts && accounts.rows)) {
                    return apiRes.status(400).send(`Что то пошло не так при авторизации`)
                }
                
                for (const account of accounts.rows) {
                    if (account.name === name) {
                        if (account.password === password) {
                            console.log(`Авторизация прошла успешно, пользователь: ${name}, пароль ${password}`)
    
                            const authToken = jwt.sign({ id: account.id, name: account.name }, authKey,
                                                       { expiresIn: tokenExp })
    
                            const userCookies = await client.query(
                                                            `select * from account_cookies where acc_id = ${ account.id }`)
                            const moduleAccess = await client.query(
                                                            `select access_modules, staff_id, roles
                                                            from account_module_access where acc_id = ${ account.id }`)
                        
                            client.release()
                            return apiRes.status(200).send({
                                authToken,
                                roleToken: getRoleToken(moduleAccess),
                                cookies: getCookies(userCookies)
                            })
                        } else {
                            client.release()
                            console.log(`Авторизация прошла не успешно для пользователя ${ name }, не верный пароль`)
                            return apiRes.status(400).send(`Авторизация прошла не успешно, не верный пароль`)
                        }
                    }
                }
            
                client.release()
                console.log(`Авторизация прошла не успешно, пользователь ${ name } не найден`)
                return apiRes.status(400).send(`Авторизация прошла не успешно, пользователь ${ name } не найден`)
            } catch(error) {
                console.log(error)
                const message = error.message === undefined ? error.routine : error.message
                apiRes.status(400).send(message)
            }
        })

        //Сохранение настроек
        app.post('/api/save-settings', (apiReq, apiRes) => {
            const { error } = _validateSettings(apiReq.body);
            if (error) return apiRes.status(400).send(error.details[0].message);

            showRequestInfoAndTime(`Получен запрос на сохранение настроек`)

            const authResult = LoginApi.checkAuth(apiReq, apiRes)
            if (!authResult) return

            const module = apiReq.body.module
            const settings = apiReq.body.settings
            const value = apiReq.body.value

            const query = `select value from account_cookies where acc_id = ${ authResult.id }
                                                    and module = '${ module }' and settings = '${ settings }'`
    
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
                
                client
                .query(query)
                .then(
                    queryResult => {
                        const selectSettings = queryResult.rows[0]
                        if (selectSettings) {
                            const query = `update account_cookies set value = '${value}'
                                                    where acc_id = ${authResult.id} and module = '${module}'
                                                    and settings = '${settings}'`

                            return { promise: client.query(query), action: 'update'}
                        } else {
                            const query = `insert into account_cookies (acc_id, module, settings, value)   
                                                 values (${ authResult.id },'${ module }','${ settings }','${ value }')`

                            return { promise: client.query(query), action: 'new'}
                        }
                    }
                ).then(
                async result => {
                    await result.promise
                    result.action === "new" ? console.log(`Настройки созданы`) : console.log(`Настройки обновлены`)

                    return apiRes.send({ action: result.action })
                }
                ).catch(
                    error => {
                        const message = error.message === undefined ? error.routine : error.message
                        return apiRes.status(400).send(message)
                    }
                )
            })
        })
    }

    static checkAuth(apiReq, apiRes) {
        if (!apiReq.headers) {
            console.log(`не получен заголовок с токеном авторизации\n`)
            apiRes.status(401).send('не получен заголовок с токеном авторизации')
        }

        const token = apiReq.headers.authorization
        //console.log(token)

        if (token && token !== 'null') {
            //console.log(`Получен токен авторизации(модуль регистрации): ${token}`)
            try {
                const authResult = jwt.verify(token, authKey)
                console.log(`Пользователь идентифицирован: ${ authResult.name }\n`)
                return authResult
            } catch (e) {
                if (e.message === 'jwt expired') {
                    console.log(`срок токена авторизации истек\n`)
                    apiRes.status(401).send('срок токена авторизации истек, авторизуйтесь заново')
                } else {
                    apiRes.status(401).send(e.message)
                }
            }
        } else {
            console.log(`срок токена авторизации истек: ${token}\n`)
            apiRes.status(401).send('срок токена авторизации истек, авторизуйтесь заново')
        }
    }
}

function _validateLogin(request) {
    const schema = {
        name: joi.string().required(),
        password: joi.string().required()
    }
    return joi.validate(request, schema);
}

function _validateSettings(settings) {
    const schema = {
        module: joi.string().required(),
        settings: joi.string().required(),
        value: joi.string().required()
    }
    return joi.validate(settings, schema);
}

function getCookies(userCookies) {
    let cookies = {}
    if (userCookies.rows && userCookies.rows.length) {
        for (const cookie of userCookies.rows) {
            if (!cookies[ cookie.module ]) {
                cookies[ cookie.module ] = []
            }
            cookies[ cookie.module ].push({ settings: cookie.settings, value: cookie.value })
        }
    }
    return cookies
}

function getRoleToken(moduleAccess) {
    let roleToken = ''
    if (moduleAccess.rows && moduleAccess.rows.length) {
        roleToken = jwt.sign(moduleAccess.rows.pop(), roleKey)
    }
    return roleToken
}