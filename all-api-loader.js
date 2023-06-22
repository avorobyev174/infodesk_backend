const MeterRegistrationApi = require('./meter_registration/meter-registration-api')
const LoginApi = require('./login/login-api')
const ChartApi = require('./charts/charts-api')
const ProfileApi = require('./profile/profile-api')
const SearchApi = require('./search/search-api')
const TestUtilsApi = require('./test-utils/test-utils-api')
const ReportsApi = require('./reports/reports-api')
const MeterStorageApi = require('./meter_storage/meter-storage-api')
const MeterRepairApi = require('./meter_repair/meter-repair-api')
const RepairAndMaterials = require('./meter_storage/modules/repair-and-materials')
const ServiceApi = require('./service/meter-service-api')
const CommonApi = require('./common/common-api')
const MapApi = require('./map/map-api')

function _loadApi(app) {
    new MeterRegistrationApi(app)
    new LoginApi(app)
    new ChartApi(app)
    new ProfileApi(app)
    new SearchApi(app)
    new TestUtilsApi(app)
    new ReportsApi(app)
    new MeterStorageApi(app)
    new MeterRepairApi(app)
    new RepairAndMaterials(app)
    new ServiceApi(app)
    new CommonApi(app)
    new MapApi(app)
}

module.exports = {
    loadInfoDeskApi: _loadApi
}

