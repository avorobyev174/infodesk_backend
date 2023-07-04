const { pgPool } = require("../database/postgres/postgres-db-connection"),
{ showRequestInfoAndTime, jwt, authKey, roleKey, joi, tokenExp, executeQuery, executePGIQuery } = require('../utils'),
{ get } = require('axios'),
moduleName = 'map',
{ checkAuth } = require('../login/login-api')

module.exports = class MapApi {
    constructor(app) {
        // app.get(`/api/${ moduleName }/geo-update`, async (apiReq, apiRes) => {
        //     showRequestInfoAndTime(`Обновление геопозиции`)
        //
        //     // if (!checkAuth(apiReq, apiRes))
        //     //     return
        //
        //     const client = await pgPool.connect()
        //     try {
        //         const addresses = await executeQuery(client,'select id, address from map_house_address')
        //         let i = 1
        //         for (const { id, address } of addresses) {
        //             //const address = 'Магнитогорск, ул. Жукова, д.14'
        //             console.log(`https://maps.googleapis.com/maps/api/geocode/
        //                 json?key=&address=${ address }`)
        //             const response = await get(
        //                 encodeURI(`https://maps.googleapis.com/maps/api/geocode/json?key=&address=${ address }`))
        //             //console.log(respAuth)
        //             //console.log(response)
        //             const { lat, lng } = response.data.results[0].geometry.location
        //             console.log(lat, lng)
        //             await executeQuery(client,`update map_house_address set lat = '${ lat }', lng = '${ lng }' where id = ${ id }`)
        //             //apiRes.send(coordinates)
        //             console.log(i)
        //             if (i % 25 === 0) {
        //                 await delay(10000)
        //             }
        //             i++
        //         }
        //         apiRes.send('s')
        //     } catch (e) {
        //         apiRes.status(400).send(e.message)
        //     } finally {
        //         client.release()
        //     }
        // })
    
        //Получение адресов
        app.get(`/api/${ moduleName }/addresses`, async (apiReq, apiRes) => {
            if (!checkAuth(apiReq, apiRes)) {
                return
            }
            
            executePGIQuery(`select * from map_house_address`, apiRes)
        })
    }
}

function delay(milliseconds){
    return new Promise(resolve => {
        setTimeout(resolve, milliseconds)
    });
}

function _validateLogin(request) {
    const schema = {
        name: joi.string().required(),
        password: joi.string().required()
    }
    return joi.validate(request, schema);
}
