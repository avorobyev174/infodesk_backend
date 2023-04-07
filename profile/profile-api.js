const { pgPool } = require("../database/postgres/postgres-db-connection"),
{ showRequestInfoAndTime, jwt, authKey, joi, tokenExp, executePGIQuery } = require('../utils'),
{ checkAuth } = require('../login/login-api'),
module_name = 'profile'

module.exports = class ProfileApi {
    constructor(app) {
        //Получение данных профиля
        app.get(`/api/${module_name}/get-info`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Получен запрос на информацию о профиле`)

            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return

            const query = `select * from accounts where id = ${ authResult.id }`
            executePGIQuery(query, apiRes)
        })
    
        app.post(`/api/${module_name}/change-password`, (apiReq, apiRes) => {
            showRequestInfoAndTime(`Получен запрос на смену пароля`)
        
            const authResult = checkAuth(apiReq, apiRes)
            if (!authResult) return
            
            const newPass = apiReq.body.newPassword
            const oldPass = apiReq.body.oldPassword
            
            pgPool.connect((connErr, client, done) => {
                if (connErr) apiRes.status(400).send(connErr.detail)
    
                client.query(`select password from accounts where id = ${ authResult.id }`)
                    .then(
                        async queryResult => {
                            const row = queryResult.rows[0]
                            if (row.password !== oldPass) throw new Error('cтарый пароль не совпадает')
                            else {
                                let updateQuery = `update accounts set password = '${ newPass }'
                                                                        where id = ${ authResult.id } returning id`
                                return { promise : client.query(updateQuery) }
                            }
                        })
                    .then(
                    async result => {
                        const updateResult = await result.promise
                        done()
                        if (updateResult.rows.length)
                            apiRes.status(200).send(updateResult.rows[0])
                        else
                            apiRes.status(400).send('Что то пошло не так при смене пароля')
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
    }
}
