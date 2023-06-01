const MeterRegistrationApi = require('./meter_registration/meter-registration-api')
const LoginApi = require('./login/login-api')
const ChartApi = require('./charts/charts-api')
const ProfileApi = require('./profile/profile-api')
const SearchApi = require('./search/search-api')
const TestUtilsApi = require('./test-utils/test-utils-api')
const ReportsApi = require('./reports/reports-api')
const MeterStorage = require('./meter_storage/meter-storage-api')
const MeterRepair = require('./meter_repair/meter-repair-api')
const RepairAndMaterials = require('./meter_storage/modules/repair-and-materials')

function _loadApi(app) {
    new MeterRegistrationApi(app)
    new LoginApi(app)
    new ChartApi(app)
    new ProfileApi(app)
    new SearchApi(app)
    new TestUtilsApi(app)
    new ReportsApi(app)
    new MeterStorage(app)
    new MeterRepair(app)
    new RepairAndMaterials(app)
}

module.exports = {
    loadInfoDeskApi: _loadApi
}

