
using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.AXAPROJECT;
using POBusinessLayerApi.DataAccessObject.Interfaces.UserInformations;
using POBusinessLayerApi.LogUtilities;
using POBusinessLayerApi.Models;
using POBusinessLayerApi.Models.ViewModels.UserInformations;
using POBusinessLayerApi.Utils;
using POBusinessLayerApi.Utils.JWT;
using SendGrid;
using SendGrid.Helpers.Mail;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace POBusinessLayerApi.DataAccessObject.Implementations.Modules.AXAPROJECT
{
    //added by sunny singh 05/03/2024
    public class AxaProjectDao : BaseDao, IAxaProjectDao
    {

        public AxaProjectDao(ISaasInformationDao<SaasInfoViewModel, int> saasDao, IUserDao<UserViewModel, int> userDao) : base(saasDao, userDao)
        {

        }

        public List<Dictionary<string, object>> AXAPROJECTImport(UserInfoModel userInfoModel, DataTable data, string Dtname)
        {

            List<Dictionary<string, object>> Data = new List<Dictionary<string, object>>();
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                using (DbContextTransaction transaction = context.Database.BeginTransaction())
                {
                    Helper.WriteInformation("AXAPROJECTImport   starts executing");

                    try
                    {
                        var ConnectionString = "";
                        int SAAS_ENABLED = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["SAAS_ENABLED"]);
                        if (SAAS_ENABLED > 0)
                        {
                            ConnectionString = _saasDao.GetByCustomerId(userInfoModel.CustomerId).CONN_STRING;
                            ConnectionString = PwCrypt.PwCrypt.DeCryptDsnPassword((ConnectionString.Contains("Provider=")
                                                ? DbUtility.excludProviderConnectionString(ConnectionString) : ConnectionString));
                        }
                        else
                        {
                            ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
                            ConnectionString = PwCrypt.PwCrypt.DeCryptDsnPassword((ConnectionString.Contains("Provider=")
                           ? DbUtility.excludProviderConnectionString(ConnectionString) : ConnectionString));
                        }

                        Data = AXAPROJECTImport(userInfoModel, data, Dtname, context, ConnectionString);

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {

                        Helper.WriteError(ex, $"Error in   AXAPROJECTImport: {ex.Message}");
                        transaction.Rollback();
                        throw ex;
                    }
                    finally
                    {

                        Helper.WriteInformation("AXAPROJECTImport finishes executing");
                    }
                }
            }

            return Data;
        }

        public List<Dictionary<string, object>> AXAPROJECTImport(UserInfoModel userInfoModel, DataTable data, string Dtname, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            var ConnectionStringorigional = System.Configuration.ConfigurationManager.ConnectionStrings["CustomConnection"].ConnectionString;
            ConnectionString = PwCrypt.PwCrypt.DeCryptDsnPassword(ConnectionStringorigional);

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                SqlTransaction trans = connection.BeginTransaction();

                Helper.WriteInformation("AXAPROJECTImport   starts executing");

                if (Dtname == "Project")
                {

                    try
                    {
                        data.Columns["Column0"].ColumnName = "Project ID";
                        data.Columns["Column1"].ColumnName = "COS";
                        data.Columns["Column2"].ColumnName = "Currency";
                        data.Columns["Column3"].ColumnName = "AXA Project Code";
                        data.Columns["Column4"].ColumnName = "Title";
                        data.Columns["Column5"].ColumnName = "Sponsor";
                        data.Columns["Column6"].ColumnName = "Line of Business";
                        data.Columns["Column7"].ColumnName = "Business Project Manager";
                        data.Columns["Column8"].ColumnName = "Strategic Pillar";
                        data.Columns["Column9"].ColumnName = "Strategic Cluster";
                        data.Columns["Column10"].ColumnName = "New/Running";
                        data.Columns["Column11"].ColumnName = "Description";
                        data.Columns["Column12"].ColumnName = "Start Date";
                        data.Columns["Column13"].ColumnName = "Finish Date";
                        data.Columns["Column14"].ColumnName = "Group Commitment";
                        data.Columns["Column15"].ColumnName = "NPS Customer";
                        data.Columns["Column16"].ColumnName = "NPS Distributors";
                        data.Columns["Column17"].ColumnName = "Regulatory";
                        data.Columns["Column18"].ColumnName = "Budget Y+1 dell'iniziativa:stima di alto livello(range)";
                        data.Columns["Column19"].ColumnName = "Budget Y+2 dell'iniziativa:stima di alto livello(range)";
                        data.Columns["Column20"].ColumnName = "Budget Y+3 dell'iniziativa:stima di alto livello(range)";
                        data.Columns["Column21"].ColumnName = "Budget Y+4 dell'iniziativa:stima di alto livello(range)";
                        data.Columns["Column22"].ColumnName = "Effort interno Y+1 dell'iniziativa:stima di alto livello in termini di FTE";
                        data.Columns["Column23"].ColumnName = "Effort interno Y+2 dell'iniziativa:stima di alto livello in termini di FTE";
                        data.Columns["Column24"].ColumnName = "Effort interno Y+3 dell'iniziativa:stima di alto livello in termini di FTE";
                        data.Columns["Column25"].ColumnName = "Effort interno Y+4 dell'iniziativa:stima di alto livello in termini di FTE";
                        data.Columns["Column26"].ColumnName = "Ranking";
                        data.Columns["Column27"].ColumnName = "Portfolio";
                        data.Columns["Column28"].ColumnName = "WBS TEMPLATE";
                        data.Columns["Column29"].ColumnName = "WBS PARENT ACTIVITY";

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "TEMP_AXA_PROJECT";
                        objbulk.ColumnMappings.Add("Project ID", "PROJECTID");
                        objbulk.ColumnMappings.Add("COS", "COS");
                        objbulk.ColumnMappings.Add("Currency", "CURRENCY");
                        objbulk.ColumnMappings.Add("AXA Project Code", "AXA_PROJECT_CODE");
                        objbulk.ColumnMappings.Add("Title", "TITLE");
                        objbulk.ColumnMappings.Add("Sponsor", "SPONSOR");
                        objbulk.ColumnMappings.Add("Line of Business", "LINE_OF_BUSS");
                        objbulk.ColumnMappings.Add("Business Project Manager", "BUSINESS_PRO_MANAGER");
                        objbulk.ColumnMappings.Add("Strategic Pillar", "STRA_PILLAR");
                        objbulk.ColumnMappings.Add("Strategic Cluster", "STRA_CLUSTER");
                        objbulk.ColumnMappings.Add("New/Running", "NEW_RUNNING");
                        objbulk.ColumnMappings.Add("Description", "DESCRIPTION");
                        objbulk.ColumnMappings.Add("Start Date", "STARTDATE");
                        objbulk.ColumnMappings.Add("Finish Date", "FINISHDATE");
                        objbulk.ColumnMappings.Add("Group Commitment", "GRO_COMMI");
                        objbulk.ColumnMappings.Add("NPS Customer", "NPS_CUST");
                        objbulk.ColumnMappings.Add("NPS Distributors", "NPS_DIST");
                        objbulk.ColumnMappings.Add("Regulatory", "REGU");
                        objbulk.ColumnMappings.Add("Budget Y+1 dell'iniziativa:stima di alto livello(range)", "BUDGET_Y_1");
                        objbulk.ColumnMappings.Add("Budget Y+2 dell'iniziativa:stima di alto livello(range)", "BUDGET_Y_2");
                        objbulk.ColumnMappings.Add("Budget Y+3 dell'iniziativa:stima di alto livello(range)", "BUDGET_Y_3");
                        objbulk.ColumnMappings.Add("Budget Y+4 dell'iniziativa:stima di alto livello(range)", "BUDGET_Y_4");
                        objbulk.ColumnMappings.Add("Effort interno Y+1 dell'iniziativa:stima di alto livello in termini di FTE", "EFFORT_Y_1");
                        objbulk.ColumnMappings.Add("Effort interno Y+2 dell'iniziativa:stima di alto livello in termini di FTE", "EFFORT_Y_2");
                        objbulk.ColumnMappings.Add("Effort interno Y+3 dell'iniziativa:stima di alto livello in termini di FTE", "EFFORT_Y_3");
                        objbulk.ColumnMappings.Add("Effort interno Y+4 dell'iniziativa:stima di alto livello in termini di FTE", "EFFORT_Y_4");
                        objbulk.ColumnMappings.Add("Ranking", "RANKING");
                        objbulk.ColumnMappings.Add("Portfolio", "PORTFOLIO");
                        objbulk.ColumnMappings.Add("WBS TEMPLATE", "WBS_TEMP");
                        objbulk.ColumnMappings.Add("WBS PARENT ACTIVITY", "WBS_PARENT_ACTI");
                        objbulk.WriteToServer(data);

                        //SqlCommand exesp = new SqlCommand("TEMP_AXA_PROJECT", connection);
                        //exesp.CommandTimeout = 1200;
                        //exesp.Transaction = trans;
                        //using (var reader = exesp.ExecuteReader())
                        //{
                        //    codes = DbUtility.Read(reader).ToList();
                        //}
                        trans.Commit();
                        //exesp.Dispose();
                    }

                    catch (Exception ex)
                    {
                        trans.Rollback();
                        throw ex;
                    }
                    finally
                    {
                        Helper.WriteInformation("AXAPROJECTImport-Project   finishes executing");
                        connection.Close();
                    }

                }
                if (Dtname == "Costi")
                {
                    try
                    {
                        data.Columns["Column0"].ColumnName = "Project ID";
                        data.Columns["Column1"].ColumnName = "Cost Category";
                        data.Columns["Column2"].ColumnName = "Capex/Opex"; 
                        data.Columns["Column3"].ColumnName = "Budget Y+1";
                         

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "TEMP_AXA_COST";
                        objbulk.ColumnMappings.Add("Project ID", "PROJECTID");
                        objbulk.ColumnMappings.Add("Cost Category", "COST_CATEGORY");
                        objbulk.ColumnMappings.Add("Capex/Opex", "CAPEX_OPEX");
                        objbulk.ColumnMappings.Add("Budget Y+1", "BUDGET_Y1");
                        
                        objbulk.WriteToServer(data);
 
                        trans.Commit();


                    }
                    catch (Exception ex)
                    {
                        trans.Rollback();
                        throw ex;
                    }
                    finally
                    {
                        Helper.WriteInformation("AXAPROJECTImport-Costi   finishes executing");
                        connection.Close();
                    }
                }
                if (Dtname == "Activities")
                {
                    try
                    {
                        data.Columns["Column0"].ColumnName = "Project ID";
                        data.Columns["Column1"].ColumnName = "Task Name";
                        data.Columns["Column2"].ColumnName = "Start";
                        data.Columns["Column3"].ColumnName = "Finish";
                        data.Columns["Column4"].ColumnName = "WBS PARENT ACTIVITY";
                        data.Columns["Column5"].ColumnName = "ACTIVITY TYPE";
                         
                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "TEMP_AXA_ACTIVITY";
                        objbulk.ColumnMappings.Add("Project ID", "PROJECTID");
                        objbulk.ColumnMappings.Add("Task Name", "TASK_NAME");
                        objbulk.ColumnMappings.Add("Start", "START");
                        objbulk.ColumnMappings.Add("Finish", "FINISH");
                        objbulk.ColumnMappings.Add("WBS PARENT ACTIVITY", "WBS_PARENT");
                        objbulk.ColumnMappings.Add("ACTIVITY TYPE", "ACTIVITY_TYPE");

                        objbulk.WriteToServer(data);

                        trans.Commit();


                    }
                    catch (Exception ex)
                    {
                        trans.Rollback();
                        throw ex;
                    }
                    finally
                    {
                        Helper.WriteInformation("AXAPROJECTImport-ACTIVITY   finishes executing");
                        connection.Close();
                    }
                }
                if (Dtname == "Benefit")
                {
                    try
                    {
                        data.Columns["Column0"].ColumnName = "Project ID"; 
                        data.Columns["Column1"].ColumnName = "Start";
                        data.Columns["Column2"].ColumnName = "Finish";
                        data.Columns["Column3"].ColumnName = "BENEFICIO";
                        data.Columns["Column4"].ColumnName = "Categoria";
                        data.Columns["Column5"].ColumnName = "Y1";
                        data.Columns["Column6"].ColumnName = "Y2";
                        data.Columns["Column7"].ColumnName = "Y3";
                        data.Columns["Column8"].ColumnName = "Y4"; 
                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "TEMP_AXA_BENEFIT";
                        objbulk.ColumnMappings.Add("Project ID", "PROJECTID");
                        objbulk.ColumnMappings.Add("Start", "START");
                        objbulk.ColumnMappings.Add("Finish", "FINISH");
                        objbulk.ColumnMappings.Add("Beneficio", "BENEFICIO");
                        objbulk.ColumnMappings.Add("Categoria", "CATEGORIA");
                        objbulk.ColumnMappings.Add("Y1", "Y1");
                        objbulk.ColumnMappings.Add("Y2", "Y2");
                        objbulk.ColumnMappings.Add("Y3", "Y3");
                        objbulk.ColumnMappings.Add("Y4", "Y4");

                        objbulk.WriteToServer(data);

                        trans.Commit();


                    }
                    catch (Exception ex)
                    {
                        trans.Rollback();
                        throw ex;
                    }
                    finally
                    {
                        Helper.WriteInformation("AXAPROJECTImport-BENEFIT   finishes executing");
                        connection.Close();
                    }
                }
                return codes;
            }

        }
    }
}