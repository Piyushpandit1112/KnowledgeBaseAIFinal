using ExcelDataReader;
using POBusinessLayerApi.Repositories.Interfaces.Modules.AXAPROJECT;
using POBusinessLayerApi.Repositories.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Repositories.Interfaces.UserInformations;
using POBusinessLayerApi.Utils.JWT;
using POBusinessLayerApi.Utils.Response;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;

namespace POBusinessLayerApi.Controllers
{

    //added by sunny singh 05/03/2024 Make the New Api  AXAPROJECTImport
    public class AXAProjectController : ApiController
    {
        private readonly  IAXAPROJECTRepository _AXAPROJECTRepository;
        private readonly UserInfo _userInfo;
        public ApiResponse response;
       
        public AXAProjectController(IAXAPROJECTRepository AxaProjectRepository,  UserInfo userInfo)
        {
            _AXAPROJECTRepository = AxaProjectRepository;
            _userInfo = userInfo;
        }

        [HttpPost]
        public IHttpActionResult AXAPROJECTImport()
        {
            var result = new HttpResponseMessage(HttpStatusCode.OK);
            if (ModelState.IsValid)
            {
                var token = ControllerContext.Request.Headers.Authorization.Parameter;
                var userInformation = _userInfo.UserInformation(token);
                var grants = _userInfo.UserPermission(userInformation, null);
                DataSet dsexcelRecords = new DataSet();
                IExcelDataReader reader = null;
                if (userInformation.Username.ToUpper() == "ADMIN@AXA.IT")
                {
                    string Exmessage = string.Empty;
                    Stream FileStream = null;
                  
                    if (userInformation != null)
                    {
                        try
                        {

                            var httpRequest = System.Web.HttpContext.Current.Request;
                            if (httpRequest.Files.Count > 1)
                            {
                                response = ResponseUtils.GetResponse(true, "Please select only one file.", null, ErrorType.None);
                                result = new HttpResponseMessage(HttpStatusCode.BadRequest);
                            }
                            if (httpRequest.Files.Count == 1)
                            {
                                DataTable data = new DataTable();
                                try
                                {
                                    FileStream = httpRequest.Files[0].InputStream;
                                    string FileName = httpRequest.Files[0].FileName;
                                    if (FileName.EndsWith(".xls"))
                                        reader = ExcelReaderFactory.CreateBinaryReader(FileStream);
                                    else if (FileName.EndsWith(".xlsm"))
                                        reader = ExcelReaderFactory.CreateReader(FileStream);
                                    else
                                        Exmessage = "The file format is not supported.";

                                    if (Exmessage != "")
                                    {
                                        response = ResponseUtils.GetResponse(false, Exmessage, null, ErrorType.OperationNotAllowed);
                                        result = new HttpResponseMessage(HttpStatusCode.UnsupportedMediaType);
                                        return Ok(response);
                                    }
                               


                                    dsexcelRecords = reader.AsDataSet();
                                    reader.Close();

                                    if (dsexcelRecords == null)
                                    {
                                        return null;
                                    }
                                    int i = 0;
                                    var dtCount = dsexcelRecords.Tables.Count;
                                    for (int j = i; j < dtCount; j++)
                                    {
                                        var Dtname = dsexcelRecords.Tables[i].TableName;
                                       
                                        if (dsexcelRecords != null && dsexcelRecords.Tables.Count > 0)
                                        {
                                            data = dsexcelRecords.Tables[i];
                                            i++;
                                            List<Dictionary<string, object>> exceldata = _AXAPROJECTRepository.AXAPROJECTImport(userInformation, data, Dtname);
                                            response = ResponseUtils.GetResponse(true, "Data imported successfully", null, ErrorType.None);
                                           

                                        }

                                    }

                                   
                                }
                                catch (Exception ex)
                                {

                                    throw new Exception(ex.Message);
                                }

                            }
                            else
                            {
                                response = ResponseUtils.GetResponse(false, "File not found", null, ErrorType.OperationNotAllowed);
                            }

                        }
                        catch (Exception e)
                        {
                            response = ResponseUtils.GetResponse(false, e.Message, null, ErrorType.OperationNotAllowed);
                            Exmessage = e.Message;
                        }

                    }
                    else
                    {
                        response = ResponseUtils.GetResponse(false, "Invalid Token", null, ErrorType.InvalidToken);
                    }
                }
                else
                {
                    response = ResponseUtils.GetResponse(false, "You don't have permission to upload document", null, ErrorType.OperationNotAllowed);
                }

            }
            else
            {
                var errors = ModelErrors.GetErrors(ModelState);
                response = ResponseUtils.GetResponse(false, "Model Not Valid", errors, ErrorType.ModelNotValid);
                result = new HttpResponseMessage(HttpStatusCode.Unauthorized);
            }
            return Ok(response);
        }


    }
}
