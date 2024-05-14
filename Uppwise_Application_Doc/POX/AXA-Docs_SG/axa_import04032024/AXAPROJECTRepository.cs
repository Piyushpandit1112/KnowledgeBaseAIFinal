using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Repositories.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Threading;
using POBusinessLayerApi.Repositories.Interfaces.Modules.AXAPROJECT;
using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.AXAPROJECT;

namespace POBusinessLayerApi.Repositories.Implementations.Modules.AXAPROJECT
{
    //added by sunny singh 05/03/2024
    public class AXAPROJECTRepository : IAXAPROJECTRepository
    {
        private IAxaProjectDao _axaProjectDao;
        public AXAPROJECTRepository(IAxaProjectDao axaProjectDao)
        {
            _axaProjectDao = axaProjectDao;
        }
      
        public List<Dictionary<string, object>> AXAPROJECTImport(UserInfoModel userInfoModel, DataTable data  , string Dtname)
        {
            return _axaProjectDao.AXAPROJECTImport(userInfoModel, data, Dtname);
        }
       
    }
}
