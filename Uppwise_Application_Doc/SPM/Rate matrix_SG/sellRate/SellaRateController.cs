using POBusinessLayerApi.Repositories.Interfaces.Modules.Bpm;
using POBusinessLayerApi.Repositories.Interfaces.Modules.Utilities;
using POBusinessLayerApi.Repositories.Interfaces.UserInformations;
using POBusinessLayerApi.Utils.JWT;
using POBusinessLayerApi.Utils.Response;
using System;
using System.Text;
using System.Web;
using System.Web.Http;
using System.Web.Http.Description;
using System.Linq;
using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.Utilities;
using Newtonsoft.Json;
using POBusinessLayerApi.Utils.Models;
using System.Configuration;

namespace POBusinessLayerApi.Controllers
{

    public class SellaRateController : ApiController
    {

        private IUtilityRepository _utilityRepository;
        private IUserRepository _userRepository;
        private readonly UserInfo _userInfo;
        private ApiResponse response;

        public SellaRateController(IUtilityRepository utilityRepository, IUserRepository UserRepository, UserInfo userInfo)
        {
            _utilityRepository = utilityRepository;
            _userRepository = UserRepository;
            _userInfo = userInfo;


        }


        [HttpPost]
        public IHttpActionResult SellaRateMatrixCustomCalculation([FromBody] AssIdNumber assIdN)
        {

              var headers = Request.Headers;
            var token = headers.GetValues("token").FirstOrDefault();
            string Getratetoken = ConfigurationManager.AppSettings["Getratetoken"];

            if (token == Getratetoken)
            {
                var IdNum = Convert.ToString(assIdN.AssId);

                if (IdNum != null)
                {
 
                    var calculationResult = _utilityRepository.SSellaRateMatrixCustomCalculation(Convert.ToInt64(IdNum));

                    var response = new ApiResponse
                    {
                        data = calculationResult,
                        success = true
                    };


                }
                else
                {
                    IdNum = "";
                      var res = _utilityRepository.sellaRateLog(  $"API is not active ", DateTime.Now, IdNum.ToString());

                }

            }
            else
            {
                //IdNum = "";
                // var res = _utilityRepository.sellaRateLog(  $" UnAuthorization", DateTime.Now, IdNum.ToString());

                var response = new ApiResponse
                {
                    success = false,
                    message = "Invalid Id",
                    data = ""
                };

            }
            return Ok(response);

        }
    }
}
