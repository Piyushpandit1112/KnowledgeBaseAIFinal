using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;

namespace POBusinessLayerApi.DataAccessObject.Interfaces.Modules.AXAPROJECT
{
    //added by sunny singh 05/03/2024
    public interface IAxaProjectDao
    { 
        List<Dictionary<string, object>> AXAPROJECTImport(UserInfoModel userInfoModel, DataTable data , string Dtname);
 

        
    }
}
