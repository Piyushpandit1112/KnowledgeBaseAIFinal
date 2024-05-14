using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace POBusinessLayerApi.Repositories.Interfaces.Modules.AXAPROJECT
{
    //added by sunny singh 05/03/2024
    public interface IAXAPROJECTRepository
    {
        
        List<Dictionary<string, object>> AXAPROJECTImport(UserInfoModel userInfoModel, DataTable data, string Dtname);
    }
}
