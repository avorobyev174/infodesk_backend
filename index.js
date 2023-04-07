const express = require('express'),
{ hostUrl } = require('./module-credentials'),
cors = require('cors'),
fs = require('fs'),
app = express(),
bodyParser = require('body-parser'),
{ loadInfoDeskApi } = require("./all-api-loader"),
https = require('https'),
IS_PROD = process.env.IS_PROD,
host = IS_PROD == 1 ? hostUrl.prod : hostUrl.dev,
port = IS_PROD == 1 ? 3030 : 3031

//console.log(process.env)

app.use(express.json());
app.use(express.urlencoded({ extended: true }))

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))

app.use(express.static('public'));
app.use('/images', express.static('images'));

app.use(
    cors({
        origin: host,
        methods: ['POST', 'PUT', 'GET', 'OPTIONS', 'HEAD', 'DELETE', 'AUTHORIZATION'],
        credentials: true,
        allowCredentials: true
    })
)

loadInfoDeskApi(app)

if (IS_PROD == 1) {
    const options = {
        key: fs.readFileSync("./cert/certificate.pem"),
        cert: fs.readFileSync("./cert/certificate.pem"),
    }
    https.createServer(options, app).listen(port, () => console.log(`${host}:${port}\n`))
} else {
    app.listen(port, () => console.log(`${host}:${port}\n`))
}





