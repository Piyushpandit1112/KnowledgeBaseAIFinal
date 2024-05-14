using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Repositories.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Utils.JWT;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Threading;

namespace POBusinessLayerApi.Repositories.Implementations.Modules.Utilities
{
    public class UtilityRepository : IUtilityRepository
    {
        private IUtilityDao _utilityDao;
        public UtilityRepository(IUtilityDao utilityDao)
        {
            _utilityDao = utilityDao;
        }
        public int FileUpload(UserInfoModel userInfoModel, byte[] files, string fileName, string contentType, int type)
        {
            return _utilityDao.FileUpload(userInfoModel, files, fileName, contentType, type);
        }
        public string SSellaRateMatrixCustomCalculation(UserInfoModel userInfoModel, long IdNum)
        {
            return _utilityDao.SSellaRateMatrixCustomCalculation( IdNum );
        }
        public List<Dictionary<string, object>> DataImport(UserInfoModel userInfoModel, DataTable data, int type, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _utilityDao.DataImport(userInfoModel, data, type, cancellationToken);
        }

        public List<Dictionary<string, object>> FerroGlobeDataImport(UserInfoModel userInfoModel, DataTable data)
        {
            return _utilityDao.FerroGlobeDataImport(userInfoModel, data);
        }
        
        public Dictionary<string, object> FileDownload(UserInfoModel userInfoModel, int FileId)
        {
            return _utilityDao.FileDownload(userInfoModel, FileId);
        }

        public string CSVDownload(UserInfoModel userInfoModel, int Type)
        {
            return _utilityDao.CSVDownload(userInfoModel, Type);
        }

        public Dictionary<string, object> saveLog(UserInfoModel userInfo,string txt, DateTime createdNow)
        {
            return _utilityDao.saveLog(userInfo, txt, createdNow);
        }
        public Dictionary<string, object> sellaRateLog(  string txt, DateTime createdNow , string IdNum)
        {
            return _utilityDao.sellaRateLog( txt, createdNow , IdNum);
        }
    }
}
