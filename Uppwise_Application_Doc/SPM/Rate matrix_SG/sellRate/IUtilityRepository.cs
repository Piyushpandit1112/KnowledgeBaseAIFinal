using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Threading;

namespace POBusinessLayerApi.Repositories.Interfaces.Modules.Utilities
{
   public interface IUtilityRepository
    {
        int FileUpload(UserInfoModel userInfoModel, byte[] files, string fileName, string contentType, int type);
        List<Dictionary<string, object>> DataImport(UserInfoModel userInfoModel, DataTable data, int type, CancellationToken cancellationToken);
        List<Dictionary<string, object>> FerroGlobeDataImport(UserInfoModel userInfoModel, DataTable data);

        Dictionary<string, object> FileDownload(UserInfoModel userInfoModel, int FileId);
       string CSVDownload(UserInfoModel userInfoModel, int Type);
        Dictionary<string, object> saveLog(UserInfoModel userInformation, string txt, DateTime now);

        Dictionary<string, object> sellaRateLog( string txt, DateTime createdNow, string idNum);
        string SSellaRateMatrixCustomCalculation(long IdNum );
     }
}
