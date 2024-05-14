using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading;

namespace POBusinessLayerApi.DataAccessObject.Interfaces.Modules.Utilities
{
    public interface IUtilityDao
    {
        int FileUpload(UserInfoModel userInfoModel, byte[] files, string fileName, string contentType, int Type);
        string SSellaRateMatrixCustomCalculation( long idNum);
        List<Dictionary<string, object>> DataImport(UserInfoModel userInfoModel, DataTable data, int type, CancellationToken cancellationToken);
        List<Dictionary<string, object>> FerroGlobeDataImport(UserInfoModel userInfoModel, DataTable data);

        Dictionary<string, object> FileDownload(UserInfoModel userInfoModel, int FileId);

        string CSVDownload(UserInfoModel userInfoModel, int Type);
        Dictionary<string, object> saveLog(UserInfoModel userInfo, string txt, DateTime createdNow);
        Dictionary<string, object> sellaRateLog(string txt, DateTime createdNow,string IdNum);
    }
}
