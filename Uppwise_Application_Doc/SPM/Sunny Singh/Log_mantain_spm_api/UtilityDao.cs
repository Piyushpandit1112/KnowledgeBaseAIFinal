using POBusinessLayerApi.DataAccessObject.Interfaces.Modules.Utilities;
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

namespace POBusinessLayerApi.DataAccessObject.Implementations.Modules.Utilities
{
    public class UtilityDao : BaseDao, IUtilityDao
    {
        private readonly string ErrorMessage = "Some error occured. Please contact your Admin.";
        public UtilityDao(ISaasInformationDao<SaasInfoViewModel, int> saasDao, IUserDao<UserViewModel, int> userDao) : base(saasDao, userDao)
        {

        }
        public List<Dictionary<string, object>> DataImport(UserInfoModel userInfoModel, DataTable data, int type, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<Dictionary<string, object>> Data = new List<Dictionary<string, object>>();
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                using (DbContextTransaction transaction = context.Database.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("DataImport procedure starts executing");
                    saveLog(userInfoModel, "DataImport procedure start executing", DateTime.Now);
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

                        if (type == 4)
                        {
                            Data = ImportRevenueAccountCodes(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 2)
                        {
                            try
                            {
                                ApplicationLock(userInfoModel, true);
                                Data = ImportCosNode(userInfoModel, data, context, ConnectionString);
                            }
                            finally
                            {
                                ApplicationLock(userInfoModel, false);
                            }
                        }
                        else if (type == 10)
                        {
                            Data = ImportBusinessLine(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 11)
                        {
                            Data = pt_tbl_tariffario_senza_prog(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 12)
                        {
                            Data = pt_tbl_tariffe_delivery(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 5)
                        {
                            Data = ImportAccountCodesOpex(userInfoModel, data, context, ConnectionString);

                        }
                        else if (type == 6)
                        {
                            Data = ImportAccountCodesCapex(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 7)
                        {
                            Data = ImportCespiti(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 8)
                        {
                            Data = ImportOKR(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 3)
                        {
                            Data = ImportCostData(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == -2)
                        {
                            Data = ImportTIPOLOGIA(userInfoModel, data, context, ConnectionString);
                        }
                        else if (type == 1)
                        {
                            try
                            {
                                ApplicationLock(userInfoModel, true);
                                Data = ImportUser(userInfoModel, data, context, ConnectionString, cancellationToken);
                            }
                            finally
                            {
                                ApplicationLock(userInfoModel, false);
                            }
                        }
                        //else if (type == 8)
                        //{
                        //    notremoveddata = ImportAccountCodesCapex(userInfoModel, data, context, ConnectionString);
                        //}
                        else
                        {
                            throw new InvalidOperationException("Method not implemented. please contact your admin");
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        saveLog(userInfoModel, "DataImport procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("DataImport procedure finishes executing");
                        transaction.Rollback();
                        throw;
                    }
                }
            }

            return Data;
        }

        public List<Dictionary<string, object>> FerroGlobeDataImport(UserInfoModel userInfoModel, DataTable data)
        {

            List<Dictionary<string, object>> Data = new List<Dictionary<string, object>>();
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                using (DbContextTransaction transaction = context.Database.BeginTransaction())
                {

                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportFerroGlobe procedure starts executing");
                    saveLog(userInfoModel, "ImportFerroGlobe procedure start executing", DateTime.Now);
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

                        Data = ImportFerroGlobe(userInfoModel, data, context, ConnectionString);

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        saveLog(userInfoModel, $"Error in function ImportFerroGlobe: {ex}", DateTime.Now);
                        Helper.WriteError(ex, $"Error in function ImportFerroGlobe: {ex.Message}");
                        transaction.Rollback();
                        throw ex;
                    }
                    finally
                    {
                        saveLog(userInfoModel, "ImportFerroGlobe procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("ImportFerroGlobe procedure finishes executing");
                    }
                }
            }

            return Data;
        }

        public List<Dictionary<string, object>> pt_tbl_tariffario_senza_prog(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {
            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("Import Sella Rate tbl senza procedure starts executing");
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans1 = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("SellaImportRate_tariffario_Senza procedure starts executing");
                    saveLog(userInfoModel, "SellaImportRate_tariffario_Senza procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        data.Columns["Column0"].ColumnName = "SOCIETA";
                        data.Columns["Column1"].ColumnName = "CLIENTE";
                        data.Columns["Column2"].ColumnName = "TIPO_RISORSA";
                        data.Columns["Column3"].ColumnName = "DATA_INIZIO";
                        data.Columns["Column4"].ColumnName = "DATA_FINE";
                        data.Columns["Column5"].ColumnName = "TARIFFA";
                        data.Columns["Column6"].ColumnName = "COD_RICHIEDENTE";
                        data.Columns["Column7"].ColumnName = "TARIFFA_DIFFERENZIATA";

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans1);
                        objbulk.DestinationTableName = "Sella_Import_rate_temp_Pt_Tbl_tariffa_Senza";
                        objbulk.ColumnMappings.Add("SOCIETA", "SOCIETA");
                        objbulk.ColumnMappings.Add("CLIENTE", "CLIENTE");
                        objbulk.ColumnMappings.Add("TIPO_RISORSA", "TIPO_RISORSA");
                        objbulk.ColumnMappings.Add("DATA_INIZIO", "START_DATE");
                        objbulk.ColumnMappings.Add("DATA_FINE", "FINISH_DATE");
                        objbulk.ColumnMappings.Add("TARIFFA", "PT_RATE");
                        objbulk.ColumnMappings.Add("COD_RICHIEDENTE", "COD_RICHIEDENTE");
                        objbulk.ColumnMappings.Add("TARIFFA_DIFFERENZIATA", "TARIFFA");
                        objbulk.WriteToServer(data);
                        SqlCommand exesp = new SqlCommand("exec [SellaImportRatetariffario_Senza] ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans1;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans1.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function SellaImportRate_tariffario_Senza: {ex.Message}");
                        saveLog(userInfoModel, "Error in function SellaImportRate_tariffario_Senza", DateTime.Now);
                         
                        trans1.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                        saveLog(userInfoModel, "SellaImportRate_tariffario_Senza procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("SellaImportRate_tariffario_Senza procedure finishes executing");
                    }
                    return codes;
                }
            }
         


        }

        public List<Dictionary<string, object>> pt_tbl_tariffe_delivery(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {
            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("Import Sella Rate tbl senza procedure starts executing");
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans1 = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("Import Sella Rate tbl senza procedure starts executing");
                    saveLog(userInfoModel, "Import Sella Rate tbl senza procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        data.Columns["Column0"].ColumnName = "TD_D_INIZIO";
                        data.Columns["Column1"].ColumnName = "TD_D_FINE";
                        data.Columns["Column2"].ColumnName = "TD_C_RISORSA";
                        data.Columns["Column3"].ColumnName = "TD_DS_COGNOME";
                        data.Columns["Column4"].ColumnName = "TD_DS_NOME";
                        data.Columns["Column5"].ColumnName = "TD_DS_TEAM";
                        data.Columns["Column6"].ColumnName = "TD_DS_DELIVERY";
                        data.Columns["Column7"].ColumnName = "TD_DS_SENIORITY";
                        data.Columns["Column8"].ColumnName = "TD_DS_TIPOLOGIA";
                        data.Columns["Column9"].ColumnName = "TD_DS_SOCIETA_CHE_PAGA";
                        data.Columns["Column10"].ColumnName = "TD_I_TARIFFA";
                        data.Columns["Column11"].ColumnName = "TD_D_INS";
                        data.Columns["Column12"].ColumnName = "TD_C_INS";

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans1);
                        objbulk.DestinationTableName = "Sella_Import_temp_Pt_tbl_tariffe_delivery_Rate";
                        objbulk.ColumnMappings.Add("TD_D_INIZIO", "Start_Date");
                        objbulk.ColumnMappings.Add("TD_D_FINE", "End_Date");
                        objbulk.ColumnMappings.Add("TD_C_RISORSA", "ResourceCode");
                        objbulk.ColumnMappings.Add("TD_DS_COGNOME", "TD_DS_COGNOME");
                        objbulk.ColumnMappings.Add("TD_DS_NOME", "TD_DS_NOME");
                        objbulk.ColumnMappings.Add("TD_DS_TEAM", "TD_DS_TEAM");
                        objbulk.ColumnMappings.Add("TD_DS_DELIVERY", "TD_DS_DELIVERY");
                        objbulk.ColumnMappings.Add("TD_DS_TIPOLOGIA", "TD_DS_TIPOLOGIA");
                        objbulk.ColumnMappings.Add("TD_DS_SOCIETA_CHE_PAGA", "TD_DS_SOCIETA_CHE_PAGA");
                        objbulk.ColumnMappings.Add("TD_I_TARIFFA", "TD_I_TARIFFA");
                        objbulk.ColumnMappings.Add("TD_D_INS", "TD_D_INS");
                        objbulk.ColumnMappings.Add("TD_C_INS", "TD_C_INS");
                        objbulk.WriteToServer(data);
                        SqlCommand exesp = new SqlCommand("exec [Sella_Import_pt_tbl_tariffe_delivery] ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans1;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans1.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function SellaImportRate_tariffe_delivery: {ex.Message}");
                        saveLog(userInfoModel, "SellaImportRate_tariffario_Senza procedure finishes executing", DateTime.Now);
                         trans1.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                        Helper.WriteInformation("SellaImportRate_tariffe_delivery procedure finishes executing");
                    }
                    return codes;
                }
            }


        }

        public List<Dictionary<string, object>> ImportBusinessLine(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {
            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportBusinessLine procedure starts executing");
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans1 = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportBusinessLine procedure starts executing");
                    saveLog(userInfoModel, "ImportBusinessLine procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        data.Columns["Column0"].ColumnName = "ID_SOGG";
                        data.Columns["Column1"].ColumnName = "DSC_SOGG";
                        data.Columns["Column2"].ColumnName = "UO_INF";
                        data.Columns["Column3"].ColumnName = "DSC_UO_INF";
                        data.Columns["Column4"].ColumnName = "UO_SUP";
                        data.Columns["Column5"].ColumnName = "DSC_UO_SUP";
                        data.Columns["Column6"].ColumnName = "MAP_DIR3";
                        data.Columns["Column7"].ColumnName = "DSC_MAP_DIR3";
                        data.Columns["Column8"].ColumnName = "MAP_DIR2";
                        data.Columns["Column9"].ColumnName = "DSC_MAP_DIR2";
                        data.Columns["Column10"].ColumnName = "MAP_DIR1";
                        data.Columns["Column11"].ColumnName = "DSC_MAP_DIR1";
                        data.Columns["Column12"].ColumnName = "Mappa_cod";
                        data.Columns["Column13"].ColumnName = "Mappa_dsc";


                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans1);
                        objbulk.DestinationTableName = "ImportBusinessLine";
                        objbulk.ColumnMappings.Add("ID_SOGG", "ID_SOGG");
                        objbulk.ColumnMappings.Add("DSC_SOGG", "DSC_SOGG");
                        objbulk.ColumnMappings.Add("UO_INF", "UO_INF");
                        objbulk.ColumnMappings.Add("DSC_UO_INF", "DSC_UO_INF");
                        objbulk.ColumnMappings.Add("UO_SUP", "UO_SUP");
                        objbulk.ColumnMappings.Add("DSC_UO_SUP", "DSC_UO_SUP");
                        objbulk.ColumnMappings.Add("MAP_DIR3", "MAP_DIR3");
                        objbulk.ColumnMappings.Add("DSC_MAP_DIR3", "DSC_MAP_DIR3");
                        objbulk.ColumnMappings.Add("MAP_DIR2", "MAP_DIR2");
                        objbulk.ColumnMappings.Add("DSC_MAP_DIR2", "DSC_MAP_DIR2");
                        objbulk.ColumnMappings.Add("MAP_DIR1", "MAP_DIR1");
                        objbulk.ColumnMappings.Add("DSC_MAP_DIR1", "DSC_MAP_DIR1");
                        objbulk.ColumnMappings.Add("Mappa_cod", "Mappa_cod");
                        objbulk.ColumnMappings.Add("Mappa_dsc", "Mappa_dsc");
                        objbulk.WriteToServer(data);
                        SqlCommand exesp = new SqlCommand("exec [ImportBusinessLineData] ", connection);
                        exesp.Transaction = trans1;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans1.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportBusinessLine: {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportBusinessLine", DateTime.Now);
                        trans1.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                        Helper.WriteInformation("ImportBusinessLine procedure finishes executing");
                    }
                    return codes;
                }
            }

        }

        public List<Dictionary<string, object>> ImportCosNode(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {
            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportCosNode procedure starts executing");
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans1 = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportCosNode procedure starts executing");
                    saveLog(userInfoModel, "ImportCosNode procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));

                        data.Columns["Column0"].ColumnName = "id";
                        data.Columns["Column1"].ColumnName = "COD_SOCIETA";
                        data.Columns["Column2"].ColumnName = "DSC_SOCIETA";
                        data.Columns["Column3"].ColumnName = "COD_AREA_BUSINESS";
                        data.Columns["Column4"].ColumnName = "DSC_AREA_BUSINESS";
                        data.Columns["Column5"].ColumnName = "COD_AREA_ATTIVITA";
                        data.Columns["Column6"].ColumnName = "DSC_AREA_ATTIVITA";
                        data.Columns["Column7"].ColumnName = "COD_AREA_INTERMEDIA";
                        data.Columns["Column8"].ColumnName = "DSC_AREA_INTERMEDIA";
                        data.Columns["Column9"].ColumnName = "ID_SOGGETTO";
                        data.Columns["Column10"].ColumnName = "DSC_SOGGETTO";
                        data.Columns["Column11"].ColumnName = "COD_ESTERNO";
                        data.Columns["Column12"].ColumnName = "ID_H2O";
                        data.Columns["Column13"].ColumnName = "RESP";
                        data.Columns["Column14"].ColumnName = "RESP_LVL";
                        data.Columns["Column15"].ColumnName = "VICE_RESP";

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans1);
                        objbulk.DestinationTableName = "CosOrganizationStructure$";
                        objbulk.ColumnMappings.Add("id", "id");
                        objbulk.ColumnMappings.Add("COD_SOCIETA", "COD_SOCIETA");
                        objbulk.ColumnMappings.Add("DSC_SOCIETA", "DSC_SOCIETA");
                        objbulk.ColumnMappings.Add("COD_AREA_BUSINESS", "COD_AREA_BUSINESS");
                        objbulk.ColumnMappings.Add("DSC_AREA_BUSINESS", "DSC_AREA_BUSINESS");
                        objbulk.ColumnMappings.Add("COD_AREA_ATTIVITA", "COD_AREA_ATTIVITA");
                        objbulk.ColumnMappings.Add("DSC_AREA_ATTIVITA", "DSC_AREA_ATTIVITA");
                        objbulk.ColumnMappings.Add("COD_AREA_INTERMEDIA", "COD_AREA_INTERMEDIA");
                        objbulk.ColumnMappings.Add("DSC_AREA_INTERMEDIA", "DSC_AREA_INTERMEDIA");
                        objbulk.ColumnMappings.Add("ID_SOGGETTO", "ID_SOGGETTO");
                        objbulk.ColumnMappings.Add("DSC_SOGGETTO", "DSC_SOGGETTO");
                        objbulk.ColumnMappings.Add("COD_ESTERNO", "COD_ESTERNO");
                        objbulk.ColumnMappings.Add("ID_H2O", "ID_H2O");
                        objbulk.ColumnMappings.Add("RESP", "RESP");
                        objbulk.ColumnMappings.Add("RESP_LVL", "RESP_LVL");
                        objbulk.ColumnMappings.Add("VICE_RESP", "VICE_RESP");
                        objbulk.WriteToServer(data);
                        SqlCommand exesp = new SqlCommand("exec [COSUpdateData] ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans1;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans1.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportCosNode: {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportCosNode", DateTime.Now);
                        trans1.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                        saveLog(userInfoModel, "ImportCosNode procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("ImportCosNode procedure finishes executing");
                    }
                    return codes;
                }
            }
            //string message = string.Empty;
            //context.Database.ExecuteSqlCommand("EXEC [COSUpdateData]");
            //return message;
        }
        public List<Dictionary<string, object>> ImportRevenueAccountCodes(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {
            string excel = string.Empty;
            saveLog(userInfoModel, "RevenueAccountCodes Import procedure start executing", DateTime.Now);
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportRevenueAccountCodes procedure starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportRevenueCostAccountCode").SingleOrDefault();

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "CODE";
                        data.Columns["Column1"].ColumnName = "DESCRIPTION";

                        foreach (DataRow item in data.Rows)
                        {
                            item["DESCRIPTION"] = item["DESCRIPTION"].ToString().Trim();
                        }

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);

                        objbulk.DestinationTableName = "temp_ImportRevenueCostAccountCode";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("CODE", "CODE");
                        objbulk.ColumnMappings.Add("DESCRIPTION", "DESCRIPTION");
                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec importrevenueaccountcodes ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportRevenueAccountCodes: {ex.Message}");
                        trans.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }

            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportRevenueCostAccountCode where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportRevenueAccountCodes_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);

            Helper.WriteInformation("ImportRevenueAccountCodes procedure finishes executing");

            return codes;
        }

        public List<Dictionary<string, object>> ImportOKR(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string message = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportOKR procedure starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportOKR").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportOKR procedure starts executing");
                    saveLog(userInfoModel, "ImportOKR procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        // data.Columns["Column0"].ColumnName = "Extraction_Date";
                        data.Columns["Column1"].ColumnName = "Company";
                        data.Columns["Column2"].ColumnName = "Objective_Name";
                        data.Columns["Column3"].ColumnName = "Objective_ID";
                        data.Columns["Column4"].ColumnName = "Objective_period";
                        data.Columns["Column5"].ColumnName = "Objective_Owner";
                        data.Columns["Column8"].ColumnName = "KR_ID";
                        data.Columns["Column9"].ColumnName = "KR_Period";
                        data.Columns["Column10"].ColumnName = "KR_Name";

                        data.Columns["Column12"].ColumnName = "KR_Progress";
                        data.Columns["Column13"].ColumnName = "KR_Goal";
                        data.Columns["Column14"].ColumnName = "U.M.";
                        data.Columns["Column16"].ColumnName = "KR_Last_Updated";
                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "temp_ImportOKR";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        //   objbulk.ColumnMappings.Add("Extraction_Date", "Extraction_Date");
                        objbulk.ColumnMappings.Add("Company", "Company");
                        objbulk.ColumnMappings.Add("Objective_Name", "Objective_Name");
                        objbulk.ColumnMappings.Add("Objective_ID", "Objective_ID");
                        objbulk.ColumnMappings.Add("Objective_period", "Objective_period");
                        objbulk.ColumnMappings.Add("KR_ID", "KR_ID");
                        objbulk.ColumnMappings.Add("KR_Period", "KR_Period");
                        objbulk.ColumnMappings.Add("Objective_Owner", "Objective_Owner");
                        objbulk.ColumnMappings.Add("KR_Name", "KR_Name");
                        objbulk.ColumnMappings.Add("KR_Progress", "KR_Progress");
                        objbulk.ColumnMappings.Add("KR_Goal", "KR_Goal");
                        objbulk.ColumnMappings.Add("U.M.", "U.M.");
                        objbulk.ColumnMappings.Add("KR_Last_Updated", "KR_Last_Updated");

                        //DataRow[] rslt = data.Select("KR_Last_Updated <> ''");

                        foreach (DataRow item in data.Rows)
                        {
                            if (item["KR_Last_Updated"].ToString() == "" || item["KR_Last_Updated"].ToString().Trim() == "")
                            {
                                item["KR_Last_Updated"] = DBNull.Value;
                            }
                            else
                            {
                                item["KR_Last_Updated"] = Convert.ToDateTime(item["KR_Last_Updated"]);
                            }
                        }

                        objbulk.WriteToServer(data);
                        trans.Commit();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportOKR (part 1): {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportOKR", DateTime.Now);
                        trans.Rollback();
                        throw; // new Exception(ex.Message + " ||");
                    }
                    finally
                    {
                        saveLog(userInfoModel, "ImportOKR procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("ImportOKR procedure finishes executing");
                        connection.Close();
                    }
                }
            }

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    try
                    {
                        SqlCommand exesp = new SqlCommand("exec ImportOKR " + userInfoModel.Account + " ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans.Commit();
                        exesp.Dispose();

                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportOKR (part 2): {ex.Message}");
                        trans.Rollback();
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportOKR where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportOKR_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);

            Helper.WriteInformation("ImportOKR procedure finishes executing");

            return codes;
            //SqlConnection con = new SqlConnection(ConnectionString);
            //try
            //{
            //    System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
            //    newColumn.DefaultValue = ID;
            //    data.Columns.Add(newColumn);
            //    data.Columns["Column0"].ColumnName = "Extraction_Date";
            //    data.Columns["Column1"].ColumnName = "Company";
            //    data.Columns["Column2"].ColumnName = "Objective_Name";
            //    data.Columns["Column3"].ColumnName = "Objective_ID";
            //    data.Columns["Column4"].ColumnName = "Objective_period";
            //    data.Columns["Column8"].ColumnName = "KR_ID";
            //    data.Columns["Column9"].ColumnName = "KR_Period";
            //    data.Columns["Column10"].ColumnName = "KR_Name";
            //    data.Columns["Column12"].ColumnName = "KR_Progress";

            //    data.Columns["Column13"].ColumnName = "KR_Goal";
            //    data.Columns["Column14"].ColumnName = "U.M.";
            //    data.Columns["Column16"].ColumnName = "KR_Last_Updated";

            //    SqlBulkCopy objbulk = new SqlBulkCopy(con);
            //    objbulk.DestinationTableName = "temp_ImportOKR";
            //    objbulk.ColumnMappings.Add("ID", "ID");
            //    objbulk.ColumnMappings.Add("Extraction_Date", "Extraction_Date");
            //    objbulk.ColumnMappings.Add("Company", "Company");
            //    objbulk.ColumnMappings.Add("Objective_Name", "Objective_Name");
            //    objbulk.ColumnMappings.Add("Objective_ID", "Objective_ID");
            //    objbulk.ColumnMappings.Add("Objective_period", "Objective_period");
            //    objbulk.ColumnMappings.Add("KR_ID", "KR_ID");
            //    objbulk.ColumnMappings.Add("KR_Period", "KR_Period");
            //    objbulk.ColumnMappings.Add("KR_Name", "KR_Name");
            //    objbulk.ColumnMappings.Add("KR_Progress", "KR_Progress");
            //    objbulk.ColumnMappings.Add("KR_Goal", "KR_Goal");
            //    objbulk.ColumnMappings.Add("U.M.", "U.M.");
            //    objbulk.ColumnMappings.Add("KR_Last_Updated", "KR_Last_Updated");
            //    con.Open();
            //    objbulk.WriteToServer(data);
            //    con.Close();
            //}
            //catch (Exception ex)
            //{
            //    con.Close();
            //    throw new Exception(ex.Message);
            //}


            //return codes;
        }

        public List<Dictionary<string, object>> ImportFerroGlobe(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                SqlTransaction trans = connection.BeginTransaction();

                //added by Sunny Singh log  01/02/24
                Helper.WriteInformation("ImportFerroGlobe procedure starts executing");
                saveLog(userInfoModel, "ImportFerroGlobe procedure start executing", DateTime.Now);
                try
                {
                    data.Columns["Column0"].ColumnName = "ProjectCode";
                    data.Columns["Column1"].ColumnName = "Activity";
                    data.Columns["Column2"].ColumnName = "CostAccountCode";
                    data.Columns["Column3"].ColumnName = "CapexOpex";
                    data.Columns["Column4"].ColumnName = "ProjectCurrency";
                    data.Columns["Column5"].ColumnName = "ActualsValue";
                    data.Columns["Column6"].ColumnName = "DateOfExpense";

                    SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                    objbulk.DestinationTableName = "temp_FerroGlobe";
                    objbulk.ColumnMappings.Add("ProjectCode", "Project_Code");
                    objbulk.ColumnMappings.Add("Activity", "Activity");
                    objbulk.ColumnMappings.Add("CostAccountCode", "Cost_Account_Code");
                    objbulk.ColumnMappings.Add("CapexOpex", "Capex_Opex");
                    objbulk.ColumnMappings.Add("ProjectCurrency", "Project_Currency");
                    objbulk.ColumnMappings.Add("ActualsValue", "Actuals_Value");
                    objbulk.ColumnMappings.Add("DateOfExpense", "Date_Of_Expense");
                    objbulk.WriteToServer(data);

                    SqlCommand exesp = new SqlCommand("exec ImportFerroGlobe", connection);
                    exesp.CommandTimeout = 1200;
                    exesp.Transaction = trans;
                    using (var reader = exesp.ExecuteReader())
                    {
                        codes = DbUtility.Read(reader).ToList();
                    }
                    trans.Commit();
                    exesp.Dispose();
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    saveLog(userInfoModel, "Error in function ImportFerroGlobe", DateTime.Now);
                    throw ex;
                }
                finally
                {
                    saveLog(userInfoModel, "ImportFerroGlobe procedure finishes executing", DateTime.Now);
                    Helper.WriteInformation("ImportFerroGlobe procedure finishes executing");
                    connection.Close();
                }
            }
            return codes;
        }

        public List<Dictionary<string, object>> ImportTIPOLOGIA(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportTIPOLOGIA procedure starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_TIPOLOGIA").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportTIPOLOGIA procedure starts executing");
                    saveLog(userInfoModel, "ImportTIPOLOGIA procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "PROGETTO_COD";
                        data.Columns["Column2"].ColumnName = "TIPOLOGIA";
                        data.Columns["Column3"].ColumnName = "TIPOLOGIA_MANUTENZIONE";

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "temp_TIPOLOGIA";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("PROGETTO_COD", "PROGETTO_COD");
                        objbulk.ColumnMappings.Add("TIPOLOGIA", "TIPOLOGIA");
                        objbulk.ColumnMappings.Add("TIPOLOGIA_MANUTENZIONE", "TIPOLOGIA_MANUTENZIONE");
                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec ImportTIPOLOGIA ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            //codes = DbUtility.Read(reader).ToList();
                        }
                        trans.Commit();
                        exesp.Dispose();

                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportTIPOLOGIA: {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportTIPOLOGIA:", DateTime.Now);
                        trans.Rollback();
                        throw;
                    }
                    finally
                    {
                        saveLog(userInfoModel, "ImportTIPOLOGIA procedure finishes executing", DateTime.Now);
                        Helper.WriteInformation("ImportTIPOLOGIA procedure finishes executing");
                        connection.Close();
                    }
                }
            }
            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_TIPOLOGIA where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);

            Helper.WriteInformation("ImportTIPOLOGIA procedure finishes executing");

            return codes;
        }

        public List<Dictionary<string, object>> ImportUser(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportUser function starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportUser").SingleOrDefault();
            Helper.WriteInformation("ImportUser function: max ID temp user fetched correctly");
            //added by Sunny Singh log  01/02/24
             saveLog(userInfoModel, "ImportUser function start executing", DateTime.Now);
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                connection.InfoMessage += (sender, args) =>
                {
                    foreach (var err in args.Errors)
                    {
                        var sqlEvent = err as SqlError;
                        if (sqlEvent != null)
                        {
                            Helper.WriteInformation($"[SP {sqlEvent.Procedure}] - {sqlEvent.Message}");
                        }
                    }
                };

                Helper.WriteInformation("ImportUser connection opened");
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    Helper.WriteInformation("ImportUser function transaction started");

                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column2"].ColumnName = "USER_NAME";
                        data.Columns["Column4"].ColumnName = "FIRST_NAME";
                        data.Columns["Column5"].ColumnName = "LAST_NAME";
                        data.Columns["Column6"].ColumnName = "USER_EMAIL";
                        data.Columns["Column7"].ColumnName = "COS";
                        data.Columns["Column11"].ColumnName = "LOCATION";
                        data.Columns["Column12"].ColumnName = "USER_TYPE";
                        data.Columns["Column14"].ColumnName = "CONTRATTO";

                        Helper.WriteInformation("ImportUser bulkCopy starting");
                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);
                        objbulk.DestinationTableName = "temp_ImportUser";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("USER_NAME", "USER_NAME");
                        objbulk.ColumnMappings.Add("FIRST_NAME", "FIRST_NAME");
                        objbulk.ColumnMappings.Add("LAST_NAME", "LAST_NAME");
                        objbulk.ColumnMappings.Add("USER_EMAIL", "USER_EMAIL");
                        objbulk.ColumnMappings.Add("COS", "COS");
                        objbulk.ColumnMappings.Add("LOCATION", "LOCATION");
                        objbulk.ColumnMappings.Add("USER_TYPE", "USER_TYPE");
                        objbulk.ColumnMappings.Add("CONTRATTO", "CONTRATTO");
                        objbulk.WriteToServer(data);
                        Helper.WriteInformation("ImportUser bulkCopy completed");

                        Helper.WriteInformation("ImportUser stored procedure executing now");
                        using (SqlCommand exesp = new SqlCommand("SET NOCOUNT ON; EXEC UserImport", connection))
                        {
                            cancellationToken.Register(() => exesp?.Cancel());
                            exesp.CommandTimeout = 2400;
                            exesp.Transaction = trans;
                            using (var reader = exesp.ExecuteReader())
                            {
                                codes = DbUtility.Read(reader).ToList();
                            }
                            Helper.WriteInformation("ImportUser stored procedure completed");

                            Helper.WriteInformation("ImportUser start transaction commit");
                            trans.Commit();
                            Helper.WriteInformation("ImportUser end transaction commit");
                            exesp?.Dispose();
                        }
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportUser: {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportUser", DateTime.Now);
                        Helper.WriteInformation("ImportUser start transaction rollback");
                        trans.Rollback();
                        Helper.WriteInformation("ImportUser end transaction rollback");
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                        saveLog(userInfoModel, "ImportUser connection closed", DateTime.Now);
                        Helper.WriteInformation("ImportUser connection closed");
                    }
                }
            }

            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);

            Helper.WriteInformation("ImportUser function deleting temp_ImportUser data");
            string deletesql = "delete from temp_ImportUser where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            Helper.WriteInformation("ImportUser function deleting temp_ImportUser data completed");

            Helper.WriteInformation("ImportUser function deleting UserImport_Log data");
            deletesql = "delete from UserImport_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            Helper.WriteInformation("ImportUser function deleting UserImport_Log data completed");

            Helper.WriteInformation("ImportUser select finishes executing");
            return codes;
        }

        public List<Dictionary<string, object>> ImportCostData(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportCostData procedure starts executing");

            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportCostPhasing").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction transt = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportCostData procedure starts executing");
                    saveLog(userInfoModel, "ImportCostData procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "CLASSIFICAZIONE_COGE";
                        data.Columns["Column1"].ColumnName = "PROGETTO";
                        data.Columns["Column2"].ColumnName = "GENNAIO";
                        data.Columns["Column3"].ColumnName = "FEBBRAIO";
                        data.Columns["Column4"].ColumnName = "MARZO";
                        data.Columns["Column5"].ColumnName = "APRILE";
                        data.Columns["Column6"].ColumnName = "MAGGIO";
                        data.Columns["Column7"].ColumnName = "GIUGNO";
                        data.Columns["Column8"].ColumnName = "LUGLIO";
                        data.Columns["Column9"].ColumnName = "AGOSTO";
                        data.Columns["Column10"].ColumnName = "SETTEMBRE";

                        data.Columns["Column11"].ColumnName = "OTTOBRE";
                        data.Columns["Column12"].ColumnName = "NOVEMBRE";
                        data.Columns["Column13"].ColumnName = "DICEMBRE";
                        data.Columns["Column14"].ColumnName = "ANNO";
                        data.Columns["Column15"].ColumnName = "FORNITORE_CODICE";
                        data.Columns["Column16"].ColumnName = "FORNITORE_DESCRIZIONE";

                        //object type = data.Rows[0][1].GetType();

                        foreach (DataRow item in data.Rows)
                        {
                            if (item["GENNAIO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["GENNAIO"]) >0)
                                {
                                    item["GENNAIO"] = Convert.ToDecimal(item["GENNAIO"]) * -1;
                                }
                                else
                                {
                                item["GENNAIO"] = Math.Abs(Convert.ToDecimal(item["GENNAIO"]));
                                }
                            }
                            else
                            {
                                item["GENNAIO"] = DBNull.Value;
                            }

                            if (item["FEBBRAIO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["FEBBRAIO"]) > 0)
                                {
                                    item["FEBBRAIO"] = Convert.ToDecimal(item["FEBBRAIO"])*-1;
                                }
                                else
                                {
                                    item["FEBBRAIO"] = Math.Abs(Convert.ToDecimal(item["FEBBRAIO"]));

                                }
                                
                            }
                            else
                            {
                                item["FEBBRAIO"] = DBNull.Value;
                            }

                            if (item["MARZO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["MARZO"]) > 0)
                                {
                                    item["MARZO"] = Convert.ToDecimal(item["MARZO"])*-1;
                                }
                                else
                                {
                                    item["MARZO"] = Math.Abs(Convert.ToDecimal(item["MARZO"]));

                                }
                               
                            }
                            else
                            {
                                item["MARZO"] = DBNull.Value;
                            }

                            if (item["APRILE"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["APRILE"]) > 0)
                                {
                                    item["APRILE"] = Convert.ToDecimal(item["APRILE"])*-1;
                                }
                                else
                                {
                                    item["APRILE"] = Math.Abs(Convert.ToDecimal(item["APRILE"]));

                                }
                                
                            }
                            else
                            {
                                item["APRILE"] = DBNull.Value;
                            }

                            if (item["MAGGIO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["MAGGIO"]) > 0)
                                {
                                    item["MAGGIO"] = Convert.ToDecimal(item["MAGGIO"])*-1;
                                }
                                else
                                {
                                    item["MAGGIO"] = Math.Abs(Convert.ToDecimal(item["MAGGIO"]));

                                }
                               
                            }
                            else
                            {
                                item["MAGGIO"] = DBNull.Value;
                            }

                            if (item["GIUGNO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["GIUGNO"]) > 0)
                                {
                                    item["GIUGNO"] = Convert.ToDecimal(item["GIUGNO"])*-1;
                                }
                                else
                                {
                                    item["GIUGNO"] = Math.Abs(Convert.ToDecimal(item["GIUGNO"]));

                                }
                               
                            }
                            else
                            {
                                item["GIUGNO"] = DBNull.Value;
                            }

                            if (item["LUGLIO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["LUGLIO"]) > 0)
                                {
                                    item["LUGLIO"] = Convert.ToDecimal(item["LUGLIO"])*-1;
                                }
                                else
                                {
                                    item["LUGLIO"] = Math.Abs(Convert.ToDecimal(item["LUGLIO"]));

                                }
                              
                            }
                            else
                            {
                                item["LUGLIO"] = DBNull.Value;
                            }

                            if (item["AGOSTO"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["AGOSTO"]) > 0)
                                {
                                    item["AGOSTO"] = Convert.ToDecimal(item["AGOSTO"])*-1;
                                }
                                else
                                {
                                    item["AGOSTO"] = Math.Abs(Convert.ToDecimal(item["AGOSTO"]));

                                }

                              
                            }
                            else
                            {
                                item["AGOSTO"] = DBNull.Value;
                            }

                            if (item["SETTEMBRE"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["SETTEMBRE"]) > 0)
                                {
                                    item["SETTEMBRE"] = Convert.ToDecimal(item["SETTEMBRE"])*-1;
                                }
                                else
                                {
                                    item["SETTEMBRE"] = Math.Abs(Convert.ToDecimal(item["SETTEMBRE"]));

                                }

                               
                            }
                            else
                            {
                                item["SETTEMBRE"] = DBNull.Value;
                            }

                            if (item["OTTOBRE"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["OTTOBRE"]) > 0)
                                {
                                    item["OTTOBRE"] = Convert.ToDecimal(item["OTTOBRE"])*-1;
                                }
                                else
                                {
                                    item["OTTOBRE"] = Math.Abs(Convert.ToDecimal(item["OTTOBRE"]));

                                }
                               
                            }
                            else
                            {
                                item["OTTOBRE"] = DBNull.Value;
                            }

                            if (item["NOVEMBRE"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["NOVEMBRE"]) > 0)
                                {
                                    item["NOVEMBRE"] = Convert.ToDecimal(item["NOVEMBRE"])*-1;
                                }
                                else
                                {
                                    item["NOVEMBRE"] = Math.Abs(Convert.ToDecimal(item["NOVEMBRE"]));

                                }
                               
                            }
                            else
                            {
                                item["NOVEMBRE"] = DBNull.Value;
                            }

                            if (item["DICEMBRE"].ToString().Trim() != "")
                            {
                                if (Convert.ToDecimal(item["DICEMBRE"]) > 0)
                                {
                                    item["DICEMBRE"] = Convert.ToDecimal(item["DICEMBRE"])*-1;
                                }
                                else
                                {
                                    item["DICEMBRE"] = Math.Abs(Convert.ToDecimal(item["DICEMBRE"]));

                                }
                              
                            }
                            else
                            {
                                item["DICEMBRE"] = DBNull.Value;
                            }


                        }

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transt);

                        objbulk.DestinationTableName = "temp_ImportCostPhasing";
                        objbulk.ColumnMappings.Add("CLASSIFICAZIONE_COGE", "CLASSIFICAZIONE_COGE");
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("PROGETTO", "PROGETTO");
                        objbulk.ColumnMappings.Add("GENNAIO", "GENNAIO");
                        objbulk.ColumnMappings.Add("FEBBRAIO", "FEBBRAIO");
                        objbulk.ColumnMappings.Add("MARZO", "MARZO");
                        objbulk.ColumnMappings.Add("APRILE", "APRILE");
                        objbulk.ColumnMappings.Add("MAGGIO", "MAGGIO");
                        objbulk.ColumnMappings.Add("GIUGNO", "GIUGNO");
                        objbulk.ColumnMappings.Add("LUGLIO", "LUGLIO");
                        objbulk.ColumnMappings.Add("AGOSTO", "AGOSTO");
                        objbulk.ColumnMappings.Add("SETTEMBRE", "SETTEMBRE");

                        objbulk.ColumnMappings.Add("OTTOBRE", "OTTOBRE");
                        objbulk.ColumnMappings.Add("NOVEMBRE", "NOVEMBRE");
                        objbulk.ColumnMappings.Add("DICEMBRE", "DICEMBRE");
                        objbulk.ColumnMappings.Add("ANNO", "ANNO");
                        objbulk.ColumnMappings.Add("FORNITORE_CODICE", "FORNITORE_CODICE");
                        objbulk.ColumnMappings.Add("FORNITORE_DESCRIZIONE", "FORNITORE_DESCRIZIONE");
                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec ImportCostPhasing ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = transt;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        transt.Commit();
                        exesp.Dispose();

                        SqlCommand exesp1 = new SqlCommand("exec UpdateCostASSignmentDate ", connection);
                        exesp1.CommandTimeout = 1200;
                        using (var reader1 = exesp1.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader1).ToList();
                        }

                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportCostData: {ex.Message}");
                        saveLog(userInfoModel, "Error in function ImportCostData", DateTime.Now);
                        transt.Rollback();
                        throw;
                        //Dictionary<string, object> execption = new Dictionary<string, object>();
                        //execption.Add("Date_", DateTime.Now.Date);
                        //execption.Add("Hour_", 0);
                        //execption.Add("Description_", ex.Message);
                        //execption.Add("Values_", ex.StackTrace);
                        //execption.Add("Comment_", "Please contact admin");
                        //codes.Add(execption);
                        // return codes;
                    }
                    finally
                    {
                        saveLog(userInfoModel, " ImportCostData procedure finishes executing", DateTime.Now);
                        connection.Close();
                    }
                }
            }
            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportCostPhasing where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            //  context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportCostPhasing_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            //  context.Database.ExecuteSqlCommand(deletesql);
            Helper.WriteInformation("ImportCostData procedure finishes executing");

            return codes;

        }

        public List<Dictionary<string, object>> ImportCespiti(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportCespiti procedure starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportCespiti").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportCespiti procedure starts executing");
                    saveLog(userInfoModel, "ImportCespiti procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "Societa";
                        data.Columns["Column1"].ColumnName = "ID_Soggetto";
                        data.Columns["Column2"].ColumnName = "key";
                        data.Columns["Column3"].ColumnName = "Progetto_ID";
                        data.Columns["Column4"].ColumnName = "descriz";
                        data.Columns["Column5"].ColumnName = "Val_Amm_le";
                        data.Columns["Column6"].ColumnName = "Num_Doc";
                        data.Columns["Column7"].ColumnName = "Data_Doc";
                        data.Columns["Column8"].ColumnName = "Inizio_Competenza";
                        data.Columns["Column9"].ColumnName = "Fine_Competenza";
                        data.Columns["Column10"].ColumnName = "Fornitore";
                        data.Columns["Column11"].ColumnName = "Descriz_Fornitore";

                        //object type = data.Rows[0][1].GetType();

                        foreach (DataRow item in data.Rows)
                        {
                            if (item["Data_Doc"].ToString() == "")
                            {
                                item["Data_Doc"] = DBNull.Value;
                            }
                            if (item["Inizio_Competenza"].ToString() == "")
                            {
                                item["Inizio_Competenza"] = DBNull.Value;
                            }
                            if (item["Fine_Competenza"].ToString() == "")
                            {
                                item["Fine_Competenza"] = DBNull.Value;
                            }
                        }

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);

                        objbulk.DestinationTableName = "temp_ImportCespiti";
                        objbulk.ColumnMappings.Add("Societa", "Societa");
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("ID_Soggetto", "ID_Soggetto");
                        objbulk.ColumnMappings.Add("key", "key");
                        objbulk.ColumnMappings.Add("Progetto_ID", "Progetto_ID");
                        objbulk.ColumnMappings.Add("descriz", "descriz");
                        objbulk.ColumnMappings.Add("Val_Amm_le", "Val_Amm_le");
                        objbulk.ColumnMappings.Add("Num_Doc", "Num_Doc");
                        objbulk.ColumnMappings.Add("Data_Doc", "Data_Doc");
                        objbulk.ColumnMappings.Add("Inizio_Competenza", "Inizio_Competenza");
                        objbulk.ColumnMappings.Add("Fine_Competenza", "Fine_Competenza");
                        objbulk.ColumnMappings.Add("Fornitore", "Fornitore");
                        objbulk.ColumnMappings.Add("Descriz_Fornitore", "Descriz_Fornitore");

                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec ImportCespiti ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }

                        string sql = "select UserEmail, ProjectCode,ProjectTitle from CesPitiEmailImport";
                        List<Dictionary<string, object>> EmailList = ExecuteQuerySqlCon(connection, sql, trans);
                        SendEmailsAsync(context, userInfoModel, EmailList.ToList());
                        trans.Commit();
                        exesp.Dispose();

                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportCespiti: {ex.Message}");
                        trans.Rollback();
                        throw;
                    }
                    finally
                    {
                        saveLog(userInfoModel, "ImportCespiti procedure finishes executing", DateTime.Now);
                        connection.Close();
                    }
                }
            }

            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportCespiti where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportCespiti_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);

            Helper.WriteInformation("ImportCespiti procedure finishes executing");

            return codes;
        }

        public List<Dictionary<string, object>> ImportAccountCodesCapex(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportAccountCodesCapex procedure starts executing");
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportCostAccountCodeCapex").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {

                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportAccountCodesCapex procedure starts executing");
                    saveLog(userInfoModel, "ImportAccountCodesCapex procedure start executing", DateTime.Now);
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "CODE";
                        data.Columns["Column1"].ColumnName = "DESCRIPTION";

                        foreach (DataRow item in data.Rows)
                        {
                            item["DESCRIPTION"] = item["DESCRIPTION"].ToString().Trim();
                        }

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);

                        objbulk.DestinationTableName = "temp_ImportCostAccountCodeCapex";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("CODE", "CODE");
                        objbulk.ColumnMappings.Add("DESCRIPTION", "DESCRIPTION");
                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec ImportCostAccountCodeCapex ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans.Commit();
                        exesp.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportAccountCodesCapex: {ex.Message}");
                        trans.Rollback();
                        throw;
                    }
                    finally
                    {
                        saveLog(userInfoModel, "ImportAccountCodesCapex procedure finishes executing", DateTime.Now);
                        connection.Close();
                    }
                }
            }
            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportCostAccountCodeCapex where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportCostAccountCodeCapex_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            Helper.WriteInformation("ImportAccountCodesCapex procedure finishes executing");

            return codes;

        }

        public List<Dictionary<string, object>> ImportAccountCodesOpex(UserInfoModel userInfoModel, DataTable data, PODBContext context, string ConnectionString)
        {

            string excel = string.Empty;
            List<Dictionary<string, object>> codes = new List<Dictionary<string, object>>();
            Helper.WriteInformation("ImportAccountCodesOpex procedure starts executing");
            saveLog(userInfoModel, "AccountCodesOpex Import procedure start executing", DateTime.Now);
            int ID = context.Database.SqlQuery<int>("select isnull(max(ID),0) + 1 from temp_ImportCostAccountCodeOpex").SingleOrDefault();
            IEnumerable<DataRow> erc = data.AsEnumerable().Skip(1);
            data = erc.CopyToDataTable();
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (SqlTransaction trans = connection.BeginTransaction())
                {
                    try
                    {
                        System.Data.DataColumn newColumn = new System.Data.DataColumn("ID", typeof(System.Int32));
                        newColumn.DefaultValue = ID;
                        data.Columns.Add(newColumn);
                        data.Columns["Column0"].ColumnName = "CODE";
                        data.Columns["Column1"].ColumnName = "DESCRIPTION";

                        foreach (DataRow item in data.Rows)
                        {
                            item["DESCRIPTION"] = item["DESCRIPTION"].ToString().Trim();
                        }

                        SqlBulkCopy objbulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, trans);

                        objbulk.DestinationTableName = "temp_ImportCostAccountCodeOpex";
                        objbulk.ColumnMappings.Add("ID", "ID");
                        objbulk.ColumnMappings.Add("CODE", "CODE");
                        objbulk.ColumnMappings.Add("DESCRIPTION", "DESCRIPTION");
                        objbulk.WriteToServer(data);

                        SqlCommand exesp = new SqlCommand("exec ImportCostAccountCodeOpex ", connection);
                        exesp.CommandTimeout = 1200;
                        exesp.Transaction = trans;
                        using (var reader = exesp.ExecuteReader())
                        {
                            codes = DbUtility.Read(reader).ToList();
                        }
                        trans.Commit();
                        exesp.Dispose();

                    }
                    catch (Exception ex)
                    {
                        Helper.WriteError(ex, $"Error in function ImportAccountCodesOpex: {ex.Message}");
                        trans.Rollback();
                        saveLog(userInfoModel, ex.Message, DateTime.Now);
                        throw;
                    }
                    finally
                    {
                        connection.Close();
                    }
                }
            }
            DateTime currentdate = DateTime.Now;
            currentdate = currentdate.AddMonths(-1);
            string deletesql = "delete from temp_ImportCostAccountCodeOpex where CREATION_DATE < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            deletesql = "delete from ImportCostAccountCodeOpex_Log where Log_date < " + DbUtility.QD(currentdate.ToString()) + "";
            context.Database.ExecuteSqlCommand(deletesql);
            Helper.WriteInformation("ImportAccountCodesOpex procedure finishes executing");

            return codes;
        }


        public int FileUpload(UserInfoModel userInfoModel, byte[] files, string fileName, string contentType, int Type)
        {
            int FileId = -1;
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                using (DbContextTransaction transaction = context.Database.BeginTransaction())
                {

                    //added by Sunny Singh log  01/02/24
                    Helper.WriteInformation("ImportAccountCodesOpex procedure starts executing");
                    saveLog(userInfoModel, "ImportAccountCodesOpex procedure start executing", DateTime.Now);
                    try
                    {
                        FileId = context.Database.SqlQuery<int>("select (isnull(max([FILE_ID]),0) + 1) as fileid from ASCN_Files").SingleOrDefault();
                        string sql = "insert into ASCN_Files values(@FILE_ID,@FILE_NAME,@FILE_EXTENSION,@FILE_TYPE,@FILE_DATA);";
                        SqlParameter[] @params = {
                                     new SqlParameter("@FILE_ID", SqlDbType.NVarChar) { Value = FileId  },
                                     new SqlParameter("@FILE_NAME", SqlDbType.NVarChar) { Value = fileName },
                                     new SqlParameter("@FILE_EXTENSION", SqlDbType.NVarChar) { Value = contentType },
                                     new SqlParameter("@FILE_TYPE", SqlDbType.NVarChar) { Value = Type  },
                                     new SqlParameter("@FILE_DATA", SqlDbType.VarBinary,-1) { Value = files },
                                    };

                        context.Database.ExecuteSqlCommand(sql, @params);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        saveLog(userInfoModel, ex.Message, DateTime.Now);
                        transaction.Rollback();
                        throw new Exception(ex.Message);
                    }
                }
            }
            return FileId;
        }

        public Dictionary<string, object> FileDownload(UserInfoModel userInfoModel, int FileId)
        {
            Dictionary<string, object> Files;
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                Files = ExecuteQuery(context, "select [FILE_DATA],[FILE_NAME],[FILE_EXTENSION] from ASCN_Files where [FILE_ID] = " + DbUtility.QN(FileId) + "").SingleOrDefault();

            }
            return Files;
        }

        public string CSVDownload(UserInfoModel userInfoModel, int Type)
        {
            List<Dictionary<string, object>> Data = null;
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {

                if (Type == 2)
                {
                    return ProjectExport(userInfoModel, context);
                }
                else if (Type == 1)
                {
                    return TaskExport(userInfoModel, context);
                }
                else if (Type == 5)
                {
                    return ProjectBudgetExport(userInfoModel, context);
                }
                else if (Type == 3)
                {
                    return TimesheetExport(userInfoModel, context);
                }
                else if (Type == 4)
                {
                    return ClientExport(userInfoModel, context);
                }
                else if (Type == 6)
                {
                    return ProjectActualBudgetExport(userInfoModel, context);
                }
                else if (Type == 7)
                {
                    return OKRExport(userInfoModel, context);
                }
                else if (Type == 8)
                {
                    return CespitiExport(userInfoModel, context);
                }
                else if (Type == 9)
                {
                    return AuditExport(userInfoModel, context);
                }
            }
            return "";
        }

        public string AuditExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            saveLog(userInfoModel, "AuditExport procedure starts executing", DateTime.Now);
            Helper.WriteInformation("AuditExport procedure starts executing");

            try
            {
                StringBuilder SQL = new StringBuilder();
                SQL.Append("SELECT Format(tracktime, 'dd/MM/yyyy HH:mm:ss', 'en-GB') AS ACTIVITY_DATE, \n");
                SQL.Append("       x.c_user                                          AS [USER_ID], \n");
                SQL.Append("       y.s_email                                         AS [USER_EMAIL], \n");
                SQL.Append("       module                                            AS OBJECT_TYPE \n");
                SQL.Append("FROM   audittrail x \n");
                SQL.Append("       INNER JOIN uten_t017 y \n");
                SQL.Append("               ON x.c_user = y.c_user \n");
                SQL.Append("WHERE  reportexport = 1 and x.C_AZD = 2  \n");
                SQL.Append("       AND Year(tracktime) = Year(Getdate()) \n");
                SQL.Append("       AND Day(tracktime) = Day(Dateadd(day, -1, Getdate()))  and MONTH(tracktime) = month(Getdate())  \n");
                SQL.Append("ORDER  BY x.c_user, \n");
                SQL.Append("          tracktime");
                Data = ExecuteQuery(context, SQL.ToString());

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }

                Dictionary<string, object> DataColumn = Data.FirstOrDefault();

                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }

                return csv.ToString();
            }
            catch (Exception ex)
            {
                saveLog(userInfoModel, $"Error in function AuditExport: {ex}", DateTime.Now);
                Helper.WriteError(ex, $"Error in function AuditExport: {ex.Message}");
                throw;
            }
            finally
            {
                saveLog(userInfoModel, "AuditExport procedure finishes executing", DateTime.Now);
                Helper.WriteInformation("AuditExport procedure finishes executing");
            }
        }

        public string CespitiExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("CespitiExport procedure starts executing");
            saveLog(userInfoModel, "CespitiExport procedure start executing", DateTime.Now);

            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT x.[societa]                                              AS Società, \n");
                sql.Append("       x.[id_soggetto]                                          AS ID_Soggetto, \n");
                sql.Append("       x.[key], \n");
                sql.Append("       Isnull(x.[progetto_id], '')                              AS [Progetto ID] \n");
                sql.Append("       , \n");
                sql.Append("       [Descriz], \n");
                sql.Append("       x.[val_amm_le]                                           AS \n");
                sql.Append("       [Val.Amm.le], \n");
                sql.Append("       x.[num_doc]                                              AS [Num.Doc.], \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, x.[data_doc], 103), '')          AS [Data Doc.], \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, x.[inizio_competenza], 103), '') AS \n");
                sql.Append("       [inizio competenza], \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, x.[fine_competenza], 103), '')   AS \n");
                sql.Append("       [fine competenza], \n");
                sql.Append("       Isnull(x.[fornitore], '')                                AS [Fornitore], \n");
                sql.Append("       Isnull(x.[descriz_fornitore], '')                        AS \n");
                sql.Append("       [Descriz fornitore], \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, x.data_mip, 103), '')            AS [DATA MIP], \n");
                sql.Append("       Isnull(mip, 0)                                           AS [% MIP], \n");
                sql.Append("       CASE WHEN sw_da_produzione='1' then 'SI' when sw_da_produzione='0' then 'NO' else '' end                           AS \n");
                sql.Append("       [SW DA ELIMINARE dalla produzione], \n");
                sql.Append("       Isnull(breve_eliminazione, '')                           AS \n");
                sql.Append("       [Breve relazione per ELIMINAZIONE] \n");
                sql.Append("FROM   cespiti(nolock) x ");
                Data = ExecuteQuery(context, sql.ToString());

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }

                Dictionary<string, object> DataColumn = Data.FirstOrDefault();

                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function CespitiExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function CespitiExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("CespitiExport procedure finishes executing");
                saveLog(userInfoModel, "CespitiExport procedure finishes executing", DateTime.Now);
            }
        }

        public string ClientExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("ClientExport procedure starts executing");
            saveLog(userInfoModel, "ClientExport procedure starts executing", DateTime.Now);

            try
            {
                long MaxCDR_ID = context.Database.SqlQuery<long>("select MAX(ISNULL(ItegrationLabel,0)) from CosOrganizationStructurePermanent").SingleOrDefault();

                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT DISTINCT x.OBj_ID, \n");
                sql.Append("                CONVERT(VARCHAR, Getutcdate(), 103) AS DATA_RIF, \n");
                sql.Append("                x.obj_code                          AS CODICE_PROGETTO, \n");
                sql.Append("                Isnull(x.obj_title, '')             AS PROGETTO, \n");
                sql.Append("                ''                                  AS \n");
                sql.Append("                CODICE_SOCIETA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS SOCIETA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS CODICE_AREA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS AREA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS CODICE_UNITA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS UNITA_RICHIEDENTE, \n");
                sql.Append("                ''                                  AS CODICE_GRUPPO_RICHIEDENTE \n");
                sql.Append("                , \n");
                sql.Append("                ''                                  AS \n");
                sql.Append("                GRUPPO_RICHIEDENTE, \n");
                sql.Append("                Isnull(client.c_item, '')           AS \n");
                sql.Append("                CODICE_SOTTOSIST_RICHIEDENTE, \n");
                sql.Append("                Isnull(client.s_item, '')           AS SOTTOSIST_RICHIEDENTE, \n");
                sql.Append("                Isnull(client.val, 0)               AS PERC_PAGAMENTO, \n");
                sql.Append("                Isnull(cdr.cod_esterno, '')         CDR_SOGGETTO, \n");
                sql.Append("                Isnull(inv.investment, '')          AS INVESTMENT_TYPE \n");
                sql.Append("FROM   bpm_objects x \n");
                sql.Append("       INNER JOIN prog_t056 p \n");
                sql.Append("               ON p.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT s_item, \n");
                sql.Append("                                  c_item, \n");
                sql.Append("                                  val, \n");
                sql.Append("                                  obj_id \n");
                sql.Append("                  FROM   tab_intiative_clintepegante) client \n");
                sql.Append("              ON client.obj_id = x.obj_id \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT CASE \n");
                sql.Append("                                    WHEN investmenttype = 0 THEN 'Costo' \n");
                sql.Append("                                    WHEN investmenttype = 1 THEN 'Investimento' \n");
                sql.Append("                                  END AS investment, \n");
                sql.Append("                                  obj_id \n");
                sql.Append("                  FROM   tab_intiative_clintepegante) inv \n");
                sql.Append("              ON inv.obj_id = x.obj_id \n");
                sql.Append("       LEFT JOIN (SELECT cod_esterno, \n");
                sql.Append("                         id_soggetto \n");
                sql.Append("                  FROM   cosorganizationstructurepermanent \n");
                sql.Append("                  WHERE  itegrationlabel = " + MaxCDR_ID + ") cdr \n");
                sql.Append("              ON cdr.id_soggetto = client.c_item \n");
                sql.Append("WHERE  x.f_id = 10");

                Data = ExecuteQuery(context, sql.ToString());
                List<Dictionary<string, object>> cos = ExecuteQuery(context, "exec sp_GenericclintepeganteCosWithName -1");

                //List<Dictionary<string, object>> esterno = ExecuteQuery(context, "SELECT C_ELMNT,COD_ESTERNO FROM ASCN_OBS_ESTERNO");

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();

                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                copyDataColumn.Remove("c_prog");
                copyDataColumn.Remove("OBj_ID");
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    var dict = cos.Where(x => x["OBj_ID"].ToString() == row["OBj_ID"].ToString() && x["clinetpaggente"].ToString() == row["CODICE_SOTTOSIST_RICHIEDENTE"].ToString()).ToList();
                    if (dict.Count > 0)
                    {

                        if (dict[0].ContainsKey("Società"))
                        {
                            string societa = dict[0]["Società"].ToString();
                            if (societa.Length > 2)
                            {
                                string[] _societa = societa.Split('-');
                                row["CODICE_SOCIETA_RICHIEDENTE"] = _societa[0].Trim();
                                row["SOCIETA_RICHIEDENTE"] = _societa[1].Trim();
                            }
                        }

                        if (dict[0].ContainsKey("Area"))
                        {
                            string area = dict[0]["Area"].ToString();
                            if (area.Length > 2)
                            {
                                string[] _area = area.Split('-');
                                row["CODICE_AREA_RICHIEDENTE"] = _area[0].Trim();
                                row["AREA_RICHIEDENTE"] = _area[1].Trim();


                            }
                        }


                        if (dict[0].ContainsKey("Servizio"))
                        {
                            string Servizio = dict[0]["Servizio"].ToString();
                            if (Servizio.Length > 2)
                            {
                                string[] _Servizio = Servizio.Split('-');

                                row["CODICE_UNITA_RICHIEDENTE"] = _Servizio[0].Trim();
                                row["UNITA_RICHIEDENTE"] = _Servizio[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Ufficio"))
                        {
                            string Ufficio = dict[0]["Ufficio"].ToString();
                            if (Ufficio.Length > 2)
                            {
                                string[] _Ufficio = Ufficio.Split('-');
                                row["CODICE_GRUPPO_RICHIEDENTE"] = _Ufficio[0].Trim();
                                row["GRUPPO_RICHIEDENTE"] = _Ufficio[1].Trim();
                            }
                        }

                        //if (dict[0].ContainsKey("Team"))
                        //{
                        //    string Team = dict[0]["Team"].ToString();
                        //    if (Team.Length > 2)
                        //    {
                        //        string[] _Team = Team.Split('-');
                        //        var cod_esterno = esterno.Where(x => x["C_ELMNT"].ToString() == _Team[0].ToString().Trim()).ToList();
                        //        if (cod_esterno.Count > 0)
                        //        {
                        //            row["CDR_SOGGETTO"] = cod_esterno[0]["COD_ESTERNO"].ToString();
                        //        }

                        //    }
                        //}
                    }

                    row.Remove("c_prog");
                    row.Remove("OBj_ID");
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function ClientExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function ClientExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("ClientExport procedure finishes executing");
                saveLog(userInfoModel, "ClientExport procedure finishes executing", DateTime.Now);
            }
        }

        public bool ApplicationLock(UserInfoModel userInfoModel, bool beignorrest = false)
        {
            bool response = false;
            using (var context = GetPODBContextInstance(userInfoModel.CustomerId))
            {
                if (beignorrest)
                {
                    string insertSQL = "INSERT INTO  TAB_OBS_MOVEMENT (USERID, ACTIVE_USERS,FLAG,SET_TIME,Set_Notification,NOTIFIED_USER,MovedOBS,Users_Timeout)" +
                         "values(" + userInfoModel.Account + ",1,1," + DbUtility.QD(DateTime.Now.ToString()) + ",0," + userInfoModel.Account + ",'',0)";
                    context.Database.ExecuteSqlCommand(insertSQL);
                }
                else
                {
                    context.Database.ExecuteSqlCommand("UPDATE TAB_OBS_MOVEMENT SET FLAG = 0 , Reset_Notification = 0, RESET_TIME = " + DbUtility.QD(DateTime.Now.ToString()) + "");
                }
            }
            return response;
        }

        public string TaskExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("TaskExport procedure starts executing");
            saveLog(userInfoModel, "TaskExport procedure starts executing", DateTime.Now);

            try
            {
                int duration = context.Database.SqlQuery<int>("select isnull(MINUTES_PER_DAY,480) as MINUTES_PER_DAY from LIGHT_CALENDARS where KEY_OBJ = 2 and KEY_TYPE = 'c_azd'").SingleOrDefault();
                duration = duration / 60;

                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT x.c_prog, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '') \n");
                sql.Append("       AS PV_D_DATA_RIF, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_ID_LEV2_COD, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_DS_LEV2, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_ID_LEV3_COD, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_DS_LEV3, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_ID_LEV4_COD, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_DS_LEV4, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_ID_LEV5_COD, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_DS_LEV5, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_ID_LEV6_COD, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_DS_LEV6, \n");
                sql.Append("       Isnull(x.obj_title, '') \n");
                sql.Append("       AS PV_DS_PRJ_DESCRIPTION, \n");
                sql.Append("       ( CASE \n");
                sql.Append("           WHEN f_sta = 0 THEN 'Draft' \n");
                sql.Append("           WHEN f_sta = 1 THEN 'Approved' \n");
                sql.Append("           WHEN f_sta = 2 THEN 'Open' \n");
                sql.Append("           WHEN f_sta = 3 THEN 'Closed' \n");
                sql.Append("           ELSE 'On-Hold' \n");
                sql.Append("         END ) \n");
                sql.Append("       AS PV_C_PRJ_STATUS, \n");
                sql.Append("       projecttask.pv_p_prj_percent_complete, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_ini_frc, 103), '') \n");
                sql.Append("       AS PV_D_PRJ_SCHEDULE_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_fin_frc, 103), '') \n");
                sql.Append("       AS PV_D_PRJ_SCHEDULE_FINISH, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, actual.task_act_start, 103), '') \n");
                sql.Append("       AS \n");
                sql.Append("       PV_D_PRJ_ACTUAL_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, actual.task_act_finish, 103), '') \n");
                sql.Append("       AS \n");
                sql.Append("       PV_D_PRJ_ACTUAL_FINISH, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_ini_pnf, 103), '') \n");
                sql.Append("       AS PV_D_PRJ_BASELINE_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_fin_pnf, 103), '') \n");
                sql.Append("       AS PV_D_PRJ_BASELINE_FINISH, \n");
                sql.Append("       Isnull(prtype.menu_name, '') \n");
                sql.Append("       AS PV_DS_TIPOLOGIA, \n");
                sql.Append("       Isnull(step.dsc, '') \n");
                sql.Append("       AS PV_DS_WORKFLOW, \n");
                sql.Append("       task.task_uid \n");
                sql.Append("       AS PV_ID_WORK_ID, \n");
                sql.Append("       task.task_name \n");
                sql.Append("       AS PV_DS_DESCRIPTION, \n");
                sql.Append("       Isnull(STRATEGY_PRIORITY.p_description, '') \n");
                sql.Append("       AS PV_DS_PRIORITY, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_base_start, 103), '') \n");
                sql.Append("       AS PV_D_BASELINE_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_base_finish, 103), '') \n");
                sql.Append("       AS PV_D_BASELINE_FINISH, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_start_date, 103), '') \n");
                sql.Append("       AS PV_D_SCHEDULE_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_finish_date, 103), '') \n");
                sql.Append("       AS PV_D_SCHEDULE_FINISH, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_act_start, 103), '') \n");
                sql.Append("       AS PV_D_ACTUAL_START, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.task_act_finish, 103), '') \n");
                sql.Append("       AS PV_D_ACTUAL_FINISH, \n");
                sql.Append("       task.pv_p_percent_complete, \n");
                sql.Append("       x.obj_code \n");
                sql.Append("       AS PV_ID_SHORT_NAME, \n");
                sql.Append("       Isnull(manager.s_nom, '') \n");
                sql.Append("       AS PV_S_PROJECT_MANAGER, \n");
                sql.Append("       Isnull(manager.c_user, '') \n");
                sql.Append("       AS PV_C_COD_PROJECT_MANAGER, \n");
                sql.Append("       Isnull(work.actual_work, 0) \n");
                sql.Append("       AS \n");
                sql.Append("       PV_I_ACTUAL, \n");
                sql.Append("       Isnull(work.actual_work, 0) / 7 \n");

                sql.Append("       AS PV_I_ACTUAL_GG, \n");
                sql.Append("       Isnull(masterplan.pv_ds_master_plan, '') \n");
                sql.Append("       AS PV_DS_MASTER_PLAN, \n");
                sql.Append("       Isnull(Fat.sd_id, 0) \n");
                sql.Append("       AS PV_ID_BILLING_TYPE, \n");
                sql.Append("       Isnull(Fat.fatturazione, '') \n");
                sql.Append("       AS PV_DS_BILLING_TYPE, \n");
                sql.Append("       Isnull(case when flag.pv_f_fl_innovativo = 'Yes' then 'Y' when NORMATIVO.pv_f_fl_normativo = 'No' then 'N' else '' end, '') \n");
                sql.Append("       AS PV_F_FL_INNOVATIVO, \n");
                sql.Append("       Isnull(case when NORMATIVO.pv_f_fl_normativo = 'Yes' then 'Y'  when NORMATIVO.pv_f_fl_normativo = 'No'  then 'N' else '' end, '') \n");
                sql.Append("       AS PV_F_FL_NORMATIVO, \n");
                sql.Append("       Isnull(sponser.s_nom, '') \n");
                sql.Append("       AS PV_S_SPONSOR, \n");
                sql.Append("       Isnull(sponser.c_user, '') \n");
                sql.Append("       AS PC_C_COD_SPONSOR, \n");
                sql.Append("       '' \n");
                sql.Append("       AS PV_I_MANUAL_GG, \n");
                sql.Append("       CASE \n");
                sql.Append("         WHEN objectiveid IS NULL THEN 'NO' \n");
                sql.Append("         ELSE 'YES' \n");
                sql.Append("       END \n");
                sql.Append("       AS OKR \n");
                sql.Append("FROM   bpm_objects x \n");
                sql.Append("       LEFT JOIN (SELECT Isnull(s.title, '') AS title, \n");
                sql.Append("                         x.c_prog \n");
                sql.Append("                  FROM   tab_strategy s \n");
                sql.Append("                         INNER JOIN tab_strategy_project_mapper x \n");
                sql.Append("                                 ON x.id_strategy = s.id_strategy) s \n");
                sql.Append("              ON s.c_prog = x.c_prog \n");
                sql.Append("       INNER JOIN prog_t056 p \n");
                sql.Append("               ON p.c_prog = x.c_prog \n");
                sql.Append("                  AND x.f_id = 10 \n");
                sql.Append("       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                         s_nom, \n");
                sql.Append("                         c_dip \n");
                sql.Append("                  FROM   uten_t017) sponser \n");
                sql.Append("              ON sponser.c_dip = p.s_owner \n");
                sql.Append("       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                         s_nom, \n");
                sql.Append("                         c_dip \n");
                sql.Append("                  FROM   uten_t017) manager \n");
                sql.Append("              ON manager.c_dip = p.s_prog_mgr \n");
                sql.Append("       LEFT JOIN (SELECT menu_name, \n");
                sql.Append("                         f_type, \n");
                sql.Append("                         f_id \n");
                sql.Append("                  FROM   bpm_menu \n");
                sql.Append("                  WHERE  f_id = 10) prtype \n");
                sql.Append("              ON prtype.f_type = x.f_type \n");
                sql.Append("                 AND prtype.f_id = 10 \n");
                sql.Append("       LEFT JOIN (SELECT dsc, \n");
                sql.Append("                         id_step, \n");
                sql.Append("                         c_workflow \n");
                sql.Append("                  FROM   bpm_wf_step_details) step \n");
                sql.Append("              ON step.c_workflow = x.wf_id \n");
                sql.Append("                 AND step.id_step = x.wf_step \n");
                sql.Append("       INNER JOIN (SELECT Isnull(task_pct_comp, 0) AS PV_P_PERCENT_COMPLETE, \n");
                sql.Append("                          task_name, \n");
                sql.Append("                          task_uid, \n");
                sql.Append("                          task_act_start, \n");
                sql.Append("                          task_act_finish, \n");
                sql.Append("                          task_base_start, \n");
                sql.Append("                          task_base_finish, \n");
                sql.Append("                          task_start_date, \n");
                sql.Append("                          task_finish_date, \n");
                sql.Append("                          proj_id, \n");
                sql.Append("                          task_dur \n");
                sql.Append("                   FROM   msp_tasks \n");
                sql.Append("                   WHERE  task_id > 1) task \n");
                sql.Append("               ON task.proj_id = x.c_prog \n");
                sql.Append("       INNER JOIN (SELECT Isnull(task_pct_comp, 0) AS PV_P_PRJ_PERCENT_COMPLETE, \n");
                sql.Append("                          proj_id, \n");
                sql.Append("                          task_dur, \n");
                sql.Append("                          task_uid \n");
                sql.Append("                   FROM   msp_tasks \n");
                sql.Append("                   WHERE  task_id = 1) Projecttask \n");
                sql.Append("               ON Projecttask.proj_id = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT sd_dsc AS PV_DS_MASTER_PLAN, \n");
                sql.Append("                         z.obj_id, \n");
                sql.Append("                         z.c_prog, \n");
                sql.Append("                         obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                    AND z.id_tab = 1074) masterplan \n");
                sql.Append("              ON masterplan.c_prog = x.c_prog \n");
                sql.Append("                 AND masterplan.obj_id = x.obj_id \n");
                sql.Append("                 AND masterplan.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT sd_dsc AS PV_F_FL_INNOVATIVO, \n");
                sql.Append("                         z.obj_id, \n");
                sql.Append("                         z.c_prog, \n");
                sql.Append("                         obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                    AND z.id_tab = 1058) flag \n");
                sql.Append("              ON flag.c_prog = x.c_prog \n");
                sql.Append("                 AND flag.obj_id = x.obj_id \n");
                sql.Append("                 AND flag.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS PV_F_FL_NORMATIVO, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                    AND z.id_tab = 1063) NORMATIVO \n");
                sql.Append("              ON NORMATIVO.c_prog = x.c_prog \n");
                sql.Append("                 AND NORMATIVO.obj_id = x.obj_id \n");
                sql.Append("                 AND NORMATIVO.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT p_description, \n");
                sql.Append("                         c_prog \n");
                sql.Append("                  FROM   tab_prj_requests T \n");
                sql.Append("                         LEFT JOIN tab_strategy_priority pt \n");
                sql.Append("                                ON pt.p_code = t.s_priority) STRATEGY_PRIORITY \n");
                sql.Append("              ON STRATEGY_PRIORITY.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT sd_id, \n");
                sql.Append("                         sd_dsc AS Fatturazione, \n");
                sql.Append("                         z.obj_id, \n");
                sql.Append("                         z.c_prog, \n");
                sql.Append("                         obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                    AND z.id_tab = 1065) Fat \n");
                sql.Append("              ON Fat.c_prog = x.c_prog \n");
                sql.Append("                 AND Fat.obj_id = x.obj_id \n");
                sql.Append("                 AND Fat.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN tab_strategy_project_mapper objective \n");
                sql.Append("              ON objective.c_prog = x.c_prog \n");
                sql.Append("                 AND objectiveid IS NOT NULL \n");
                sql.Append("       LEFT JOIN ((SELECT Sum(Isnull(actual_work, 0)) AS ACTUAL_WORK, \n");
                sql.Append("                          proj_id, \n");
                sql.Append("                          task_uid \n");
                sql.Append("                   FROM   msp_task_assignment_users \n");
                sql.Append("                   GROUP  BY proj_id, \n");
                sql.Append("                             task_uid)) work \n");
                sql.Append("              ON work.proj_id = x.c_prog \n");
                sql.Append("                 AND work.task_uid = task.task_uid \n");
                sql.Append("       LEFT JOIN (SELECT Min(task_act_start)  AS TASK_ACT_START, \n");
                sql.Append("                         Max(task_act_finish) AS TASK_ACT_FINISH, \n");
                sql.Append("                         proj_id \n");
                sql.Append("                  FROM   msp_tasks \n");
                sql.Append("                  WHERE  task_id > 1 \n");
                sql.Append("                  GROUP  BY proj_id) actual \n");
                sql.Append("              ON actual.proj_id = x.c_prog \n");
                sql.Append("WHERE  x.f_id = 10");

                Data = ExecuteQuery(context, sql.ToString());

                List<Dictionary<string, object>> cos = ExecuteQuery(context, "exec sp_GenericProjectCosWithName -1");

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();


                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                copyDataColumn.Remove("c_prog");
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");


                foreach (var row in Data)
                {
                    var dict = cos.Where(x => x["ProjectId"].ToString() == row["c_prog"].ToString()).ToList();
                    if (dict.Count > 0)
                    {
                        if (dict[0].ContainsKey("Società"))
                        {
                            string societa = dict[0]["Società"].ToString();
                            if (societa.Length > 2)
                            {
                                string[] _societa = societa.Split('-');
                                row["PV_ID_LEV2_COD"] = _societa[0].Trim();
                                row["PV_DS_LEV2"] = _societa[1].Trim();
                            }
                        }
                        if (dict[0].ContainsKey("Area"))
                        {
                            string area = dict[0]["Area"].ToString();
                            if (area.Length > 2)
                            {
                                string[] _area = area.Split('-');
                                row["PV_ID_LEV3_COD"] = _area[0].Trim();
                                row["PV_DS_LEV3"] = _area[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Servizio"))
                        {
                            string Servizio = dict[0]["Servizio"].ToString();
                            if (Servizio.Length > 2)
                            {
                                string[] _Servizio = Servizio.Split('-');

                                row["PV_ID_LEV4_COD"] = _Servizio[0].Trim();
                                row["PV_DS_LEV4"] = _Servizio[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Ufficio"))
                        {
                            string Ufficio = dict[0]["Ufficio"].ToString();
                            if (Ufficio.Length > 2)
                            {
                                string[] _Ufficio = Ufficio.Split('-');
                                row["PV_ID_LEV5_COD"] = _Ufficio[0].Trim();
                                row["PV_DS_LEV5"] = _Ufficio[1].Trim();
                            }
                        }
                        if (dict[0].ContainsKey("Team"))
                        {
                            string Team = dict[0]["Team"].ToString();
                            if (Team.Length > 2)
                            {
                                string[] _Team = Team.Split('-');
                                row["PV_ID_LEV6_COD"] = _Team[0].Trim();
                                row["PV_DS_LEV6"] = _Team[1].Trim();
                            }
                        }

                    }
                    row.Remove("c_prog");
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function TaskExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function TaskExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("TaskExport procedure finishes executing");
                saveLog(userInfoModel, "TaskExport procedure finishes executing", DateTime.Now);
            }
        }

        public string OKRExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("OKRExport procedure starts executing");
            saveLog(userInfoModel, "OKRExport procedure starts executing", DateTime.Now);

            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT x.c_prog, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '')   AS [Data Estrazione], \n");
                sql.Append("       x.obj_code                                        AS [Codice Progetto], \n");
                sql.Append("       x.obj_title                                       AS [Titolo Progetto], \n");
                sql.Append("       ''                                                AS ID_Società_COD, \n");
                sql.Append("       ''                                                AS DS_Società, \n");
                sql.Append("       ''                                                AS \n");
                sql.Append("       [ID_Area Business_COD], \n");
                sql.Append("       ''                                                AS [DS_Area Business], \n");
                sql.Append("       ''                                                AS \n");
                sql.Append("       [ID_Area Attività_COD], \n");
                sql.Append("       ''                                                AS [DS_Area Attività], \n");
                sql.Append("       ''                                                AS \n");
                sql.Append("       [ID_Area Intermedia_COD], \n");
                sql.Append("       ''                                                AS [DS_Area Intermedia] \n");
                sql.Append("       , \n");
                sql.Append("       ''                                                AS \n");
                sql.Append("       [ID_Soggetto_COD], \n");
                sql.Append("       ''                                                AS [DS_Soggetto], \n");
                sql.Append("       Isnull(s.[objective _name], '')                   AS [Objective _Name], \n");
                sql.Append("       Isnull([kr _name], '')                            AS [KR _Name], \n");
                sql.Append("       Isnull(c.code, '')                                AS [KR_Period], \n");
                sql.Append("       Isnull(m.progress, 0)                             AS [KR_Progress], \n");
                sql.Append("       CASE \n");
                sql.Append("         WHEN m.typekr = 1 THEN Isnull(m.targetvalue, 0) \n");
                sql.Append("         ELSE 100 \n");
                sql.Append("       END                                               AS [KR _Goal], \n");
                sql.Append("       CASE \n");
                sql.Append("         WHEN m.typekr = 1 THEN Isnull(u.s_dsc, '') \n");
                sql.Append("         ELSE '%' \n");
                sql.Append("       END                                               AS [U.M.], \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, M.modifieddate, 103), '') AS [KR _Last _Updated] \n");
                sql.Append("FROM   bpm_objects x \n");
                sql.Append("       INNER JOIN (SELECT Isnull(o.obj_title, '') AS [Objective _Name], \n");
                sql.Append("                          x.c_prog, \n");
                sql.Append("                          objectiveid, \n");
                sql.Append("                          m.okr_cycle, \n");
                sql.Append("                          m.key_goals \n");
                sql.Append("                   FROM   bpm_objects o \n");
                sql.Append("                          INNER JOIN tab_strategy_project_mapper x \n");
                sql.Append("                                  ON x.objectiveid = o.obj_id \n");
                sql.Append("                                     AND o.f_id = 3 \n");
                sql.Append("                          INNER JOIN objectivemapping m \n");
                sql.Append("                                  ON m.obj_id = x.objectiveid \n");
                sql.Append("                   WHERE  x.objectiveid IS NOT NULL) s \n");
                sql.Append("               ON s.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT x.obj_title AS [KR _Name], \n");
                sql.Append("                         r.* \n");
                sql.Append("                  FROM   bpm_collab_rel r \n");
                sql.Append("                         INNER JOIN bpm_objects x \n");
                sql.Append("                                 ON r.id2 = x.obj_id \n");
                sql.Append("                                    AND r.id1_type = 'OBJ' \n");
                sql.Append("                                    AND r.id2_type = 'KEYRESULT' \n");
                sql.Append("                                    AND f_id = 6) k \n");
                sql.Append("              ON k.id1 = s.objectiveid \n");
                sql.Append("                 AND k.id1_type = 'OBJ' \n");
                sql.Append("                 AND k.id2_type = 'KEYRESULT' \n");
                sql.Append("       LEFT JOIN (SELECT progress, \n");
                sql.Append("                         obj_id, \n");
                sql.Append("                         modifieddate, \n");
                sql.Append("                         unitmeasure, \n");
                sql.Append("                         targetvalue, \n");
                sql.Append("                         typekr \n");
                sql.Append("                  FROM   keyresultsmapping) m \n");
                sql.Append("              ON m.obj_id = k.id2 \n");
                sql.Append("       LEFT JOIN okrcycle c \n");
                sql.Append("              ON c.id = s.okr_cycle \n");
                sql.Append("       LEFT JOIN um_t024 u \n");
                sql.Append("              ON u.c_um = m.unitmeasure \n");
                sql.Append("WHERE  x.f_id = 10");


                Data = ExecuteQuery(context, sql.ToString());

                List<Dictionary<string, object>> cos = ExecuteQuery(context, "exec sp_GenericProjectCosWithName -1");

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }

                Dictionary<string, object> DataColumn = Data.FirstOrDefault();

                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                copyDataColumn.Remove("c_prog");
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    var dict = cos.Where(x => x["ProjectId"].ToString() == row["c_prog"].ToString()).ToList();

                    if (dict.Count > 0)
                    {
                        if (dict[0].ContainsKey("Società"))
                        {
                            string societa = dict[0]["Società"].ToString();
                            if (societa.Length > 2)
                            {
                                string[] _societa = societa.Split('-');
                                row["ID_Società_COD"] = _societa[0].Trim();
                                row["DS_Società"] = _societa[1].Trim();
                            }
                        }

                        if (dict[0].ContainsKey("Area"))
                        {
                            string area = dict[0]["Area"].ToString();
                            if (area.Length > 2)
                            {
                                string[] _area = area.Split('-');
                                row["ID_Area Business_COD"] = _area[0].Trim();
                                row["DS_Area Business"] = _area[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Servizio"))
                        {
                            string Servizio = dict[0]["Servizio"].ToString();
                            if (Servizio.Length > 2)
                            {
                                string[] _Servizio = Servizio.Split('-');

                                row["ID_Area Attività_COD"] = _Servizio[0].Trim();
                                row["DS_Area Attività"] = _Servizio[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Ufficio"))
                        {
                            string Ufficio = dict[0]["Ufficio"].ToString();
                            if (Ufficio.Length > 2)
                            {
                                string[] _Ufficio = Ufficio.Split('-');
                                row["ID_Area Intermedia_COD"] = _Ufficio[0].Trim();
                                row["DS_Area Intermedia"] = _Ufficio[1].Trim();
                            }
                        }

                        if (dict[0].ContainsKey("Team"))
                        {
                            string Team = dict[0]["Team"].ToString();
                            if (Team.Length > 2)
                            {
                                string[] _Team = Team.Split('-');
                                row["ID_Soggetto_COD"] = _Team[0].Trim();
                                row["DS_Soggetto"] = _Team[1].Trim();
                            }
                        }
                    }

                    row.Remove("c_prog");
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function OKRExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function OKRExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("OKRExport procedure finishes executing");
                saveLog(userInfoModel, "OKRExport procedure finishes executing", DateTime.Now);
            }
        }

        public string ProjectExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("ProjectExport procedure starts executing");
            saveLog(userInfoModel, "ProjectExport procedure starts executing", DateTime.Now);

            try
            {

                int duration = context.Database.SqlQuery<int>("select isnull(MINUTES_PER_DAY,480) as MINUTES_PER_DAY from LIGHT_CALENDARS where KEY_OBJ = 2 and KEY_TYPE = 'c_azd'").SingleOrDefault();
                duration = duration * 10;

                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT x.c_prog, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '')           AS \n");
                sql.Append("       PV_D_DATA_RIF, \n");
                sql.Append("       x.obj_code                                                AS \n");
                sql.Append("       PV_C_PROGETTO_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_ID_COMMITTENTE_1_LIV_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_DS_COMMITTENTE_1_LIVELLO, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_ID_COMMITTENTE_2_LIV_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_DS_COMMITTENTE_2_LIVELLO, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_ID_COMMITTENTE_3_LIV_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_DS_COMMITTENTE_3_LIVELLO, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_ID_COMMITTENTE_4_LIV_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_DS_COMMITTENTE_4_LIVELLO, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_ID_COMMITTENTE_5_LIV_COD, \n");
                sql.Append("       ''                                                        AS \n");
                sql.Append("       PV_DS_COMMITTENTE_5_LIVELLO, \n");
                sql.Append("       Isnull(s.title, '')                                       AS \n");
                sql.Append("       PV_DS_INIZIATIVA, \n");
                sql.Append("       Isnull(sponser.s_nom, '')                                 AS \n");
                sql.Append("       PV_DS_PROJECT_SPONSOR, \n");
                sql.Append("       Isnull(sponser.c_user, '')                                AS \n");
                sql.Append("       PV_C_COD_PROJECT_SPONSOR, \n");
                sql.Append("       x.obj_title                                               AS \n");
                sql.Append("       PV_DS_PROGETTO, \n");
                sql.Append("       Isnull(Progetto.PR_SCOPE, '')                             AS \n");
                sql.Append("       PV_DS_DESCRIZIONE, \n");
                sql.Append("       Isnull(manager.s_nom, '')                                 AS \n");
                sql.Append("       PV_DS_PROJECT_MANAGER, \n");
                sql.Append("       Isnull(manager.c_user, '')                                AS \n");
                sql.Append("       PV_C_COD_PROJECT_MANAGER, \n");
                sql.Append("       Isnull(STRATEGY_PRIORITY.p_description, '')               AS \n");
                sql.Append("       PV_DS_PRIORITA, \n");
                sql.Append("       Isnull(prtype.menu_name, '')                              AS \n");
                sql.Append("       PV_DS_TIPOLOGIA, \n");
                sql.Append("       case when x.WF_STEP = 0 then (SELECT top 1 DSC from  bpm_wf_step_details where C_WORKFLOW  = x.WF_ID order by [PRIORITY] desc) else  Isnull(step.dsc, '')   end AS \n");
                sql.Append("       PV_DS_WORKFLOW, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.TASK_BASE_START, 103), '')            AS \n");
                sql.Append("       PV_D_INIZIO_BASELINE, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_ini_frc, 103), '')            AS \n");
                sql.Append("       PV_D_INIZIO_PIANIFICATO, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, actual.task_act_start, 103), '')  AS \n");
                sql.Append("       PV_D_INIZIO_EFFETTIVO, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, task.TASK_BASE_FINISH, 103), '')            AS \n");
                sql.Append("       PV_D_FINE_BASELINE, \n");
                sql.Append("       Isnull(CONVERT(VARCHAR, p.d_fin_frc, 103), '')            AS \n");
                sql.Append("       PV_D_FINE_PIANIFICATA, \n");
                sql.Append("       case WHEN f_sta = 3 then Isnull(CONVERT(VARCHAR, actual.task_act_finish, 103) , '') else '' end AS \n");
                sql.Append("        PV_D_FINE_EFFETTIVA , \n");
                sql.Append("       ( CASE \n");
                sql.Append("           WHEN f_sta = 0 THEN 'Draft' \n");
                sql.Append("           WHEN f_sta = 1 THEN 'Approved' \n");
                sql.Append("           WHEN f_sta = 2 THEN 'Open' \n");
                sql.Append("           WHEN f_sta = 3 THEN 'Closed' \n");
                sql.Append("           ELSE 'On-Hold' \n");
                sql.Append("         END )                                                   AS PV_C_STATO, \n");
                sql.Append("       task.pv_p_percentuale, \n");
                sql.Append("       Isnull(masterplan.pv_ds_master_plan, '')                  AS \n");
                sql.Append("       PV_DS_MASTER_PLAN, \n");
                sql.Append("       Cast(Isnull(task.task_dur, 0) / 4380 AS DECIMAL(10, 2))   AS PV_I_EFFORT, \n");
                sql.Append("       Isnull(attribute.pv_ds_innovativo, '')                    AS \n");
                sql.Append("       PV_DS_INNOVATIVO, \n");
                sql.Append("       Isnull(stato.pv_ds_innovation_state, '')                  AS \n");
                sql.Append("       PV_DS_INNOVATION_STATE, \n");
                sql.Append("       Isnull(note.ca_value, '')                                 AS PV_S_NOTE, \n");
                sql.Append("       Isnull(Fat.fatturazione, '')                              AS Fatturazione \n");
                sql.Append("       , \n");
                sql.Append("       Isnull(portfolio.portfolio_title, '')                     AS \n");
                sql.Append("       DS_PORTAFOGLIO, \n");
                sql.Append("       CASE \n");
                sql.Append("         WHEN objectiveid IS NULL THEN 'NO' \n");
                sql.Append("         ELSE 'YES' \n");
                sql.Append("       END                                                       AS OBJECTIVE, \n");
                sql.Append("       Isnull(tipologia, '')                                     AS \n");
                sql.Append("       TIPOLOGIA_NORMATIVO, \n");
                sql.Append("       Isnull(investimento, '')                                  AS \n");
                sql.Append("       TIPOLOGIA_INVESTIMENTO \n");
                sql.Append("FROM   bpm_objects x \n");
                sql.Append("       LEFT JOIN (SELECT Isnull(s.obj_title, '') AS title, \n");
                sql.Append("                         x.c_prog \n");
                sql.Append("                  FROM   bpm_objects s \n");
                sql.Append("                         INNER JOIN tab_strategy_project_mapper x \n");
                sql.Append("                                 ON x.themeid = s.obj_id) s \n");
                sql.Append("              ON s.c_prog = x.c_prog \n");
                sql.Append("       INNER JOIN prog_t056 p \n");
                sql.Append("               ON p.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                         s_nom, \n");
                sql.Append("                         c_dip \n");
                sql.Append("                  FROM   uten_t017) sponser \n");
                sql.Append("              ON sponser.c_dip = p.s_owner \n");
                sql.Append("       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                         s_nom, \n");
                sql.Append("                         c_dip \n");
                sql.Append("                  FROM   uten_t017) manager \n");
                sql.Append("              ON manager.c_dip = p.s_prog_mgr \n");


                sql.Append("   LEFT JOIN (select PR_SCOPE,PR_ID,C_PROG from TAB_PRJ_REQUESTS ) Progetto  ON Progetto.c_prog = x.c_prog  AND Progetto.PR_ID = x.obj_id  \n");
                //sql.Append("                         c_prog, \n");
                //sql.Append("                         obj_id, \n");
                //sql.Append("                         obj_type \n");
                //sql.Append("                  FROM   bpm_textbox_data Progetto \n");
                //sql.Append("                  WHERE  id_tab = 1066) Progetto \n");
                //sql.Append("              ON Progetto.c_prog = x.c_prog \n");
                //sql.Append("                 AND Progetto.obj_id = x.obj_id \n");
                //sql.Append("                 AND Progetto.obj_type = x.f_type \n");


                sql.Append("       LEFT JOIN (SELECT p_description, \n");
                sql.Append("                         c_prog \n");
                sql.Append("                  FROM   tab_prj_requests T \n");
                sql.Append("                         LEFT JOIN tab_strategy_priority pt \n");
                sql.Append("                                ON pt.p_code = t.s_priority) STRATEGY_PRIORITY \n");
                sql.Append("              ON STRATEGY_PRIORITY.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT menu_name, \n");
                sql.Append("                         f_type, \n");
                sql.Append("                         f_id \n");
                sql.Append("                  FROM   bpm_menu \n");
                sql.Append("                  WHERE  f_id = 10) prtype \n");
                sql.Append("              ON prtype.f_type = x.f_type \n");
                sql.Append("                 AND prtype.f_id = 10 \n");
                sql.Append("       LEFT JOIN (SELECT dsc, \n");
                sql.Append("                         id_step, \n");
                sql.Append("                         c_workflow \n");
                sql.Append("                  FROM   bpm_wf_step_details) step \n");
                sql.Append("              ON step.c_workflow = x.wf_id \n");
                sql.Append("                 AND step.id_step = x.wf_step \n");
                sql.Append("       LEFT JOIN (SELECT Isnull(task_pct_comp, 0) AS PV_P_PERCENTUALE, \n");
                sql.Append("                         proj_id, \n");
                sql.Append("                         task_dur, \n");
                sql.Append("                         TASK_BASE_START, \n");
                sql.Append("                         TASK_BASE_FINISH, \n");
                sql.Append("                         task_act_start, \n");
                sql.Append("                         task_act_finish \n");
                sql.Append("                  FROM   msp_tasks \n");
                sql.Append("                  WHERE  TASK_UID = 1) task \n");
                sql.Append("              ON task.proj_id = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT Min(task_act_start)  AS TASK_ACT_START, \n");
                sql.Append("                         Max(task_act_finish) AS TASK_ACT_FINISH, \n");
                sql.Append("                         proj_id \n");
                sql.Append("                  FROM   msp_tasks \n");
                sql.Append("                  WHERE  task_id > 1 \n");
                sql.Append("                  GROUP  BY proj_id) actual \n");
                sql.Append("              ON actual.proj_id = x.c_prog \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS PV_DS_MASTER_PLAN, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                               INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1074) masterplan \n");
                sql.Append("              ON masterplan.c_prog = x.c_prog \n");
                sql.Append("                 AND masterplan.obj_id = x.obj_id \n");
                sql.Append("                 AND masterplan.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS PV_DS_INNOVATIVO, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1060) attribute \n");
                sql.Append("              ON attribute.c_prog = x.c_prog \n");
                sql.Append("                 AND attribute.obj_id = x.obj_id \n");
                sql.Append("                 AND attribute.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS PV_DS_INNOVATION_STATE, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                    INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1062) stato \n");
                sql.Append("              ON stato.c_prog = x.c_prog \n");
                sql.Append("                 AND stato.obj_id = x.obj_id \n");
                sql.Append("                 AND stato.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS Fatturazione, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                     INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1065) Fat \n");
                sql.Append("              ON Fat.c_prog = x.c_prog \n");
                sql.Append("                 AND Fat.obj_id = x.obj_id \n");
                sql.Append("                 AND Fat.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS Tipologia, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                     INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1064) Tipologia \n");
                sql.Append("              ON Tipologia.c_prog = x.c_prog \n");
                sql.Append("                 AND Tipologia.obj_id = x.obj_id \n");
                sql.Append("                 AND Tipologia.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT sd_dsc AS investimento, \n");
                sql.Append("                                  z.obj_id, \n");
                sql.Append("                                  z.c_prog, \n");
                sql.Append("                                  obj_type \n");
                sql.Append("                  FROM   bpm_static_data x \n");
                sql.Append("                         INNER JOIN bpm_combo_data z \n");
                sql.Append("                                 ON x.sd_id = z.ca_value \n");
                sql.Append("                                     INNER JOIN BPM_FORM_FIELD_MASTER y ON z.IDFIELD=y.VCA_ID and y.F_TYPE=z.OBJ_TYPE where z.id_tab = 1071) investimento \n");
                sql.Append("              ON investimento.c_prog = x.c_prog \n");
                sql.Append("                 AND investimento.obj_id = x.obj_id \n");
                sql.Append("                 AND investimento.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT ca_value, \n");
                sql.Append("                         c_prog, \n");
                sql.Append("                         obj_id, \n");
                sql.Append("                         obj_type \n");
                sql.Append("                  FROM   bpm_texteditor_data x  INNER JOIN BPM_FORM_FIELD_MASTER z ON \n");
                sql.Append("                   x.IDFIELD= z.VCA_ID AND z.F_TYPE=x.OBJ_TYPE WHERE  id_tab = 1067) note \n");
                sql.Append("              ON note.c_prog = x.c_prog \n");
                sql.Append("                 AND note.obj_id = x.obj_id \n");
                sql.Append("                 AND note.obj_type = x.f_type \n");
                sql.Append("       LEFT JOIN (SELECT DISTINCT portfolio_title, \n");
                sql.Append("                                  c_prog \n");
                sql.Append("                  FROM   pf_tab_portfolios pf \n");
                sql.Append("                         INNER JOIN pf_tab_projects_ascn PP \n");
                sql.Append("                                 ON pp.portfolio_code = pf.portfolio_code) \n");
                sql.Append("                 portfolio \n");
                sql.Append("              ON portfolio.c_prog = x.c_prog \n");
                sql.Append("       LEFT JOIN tab_strategy_project_mapper objective \n");
                sql.Append("              ON objective.c_prog = x.c_prog \n");
                sql.Append("                 AND objectiveid IS NOT NULL \n");
                sql.Append("WHERE  x.f_id = 10");

                Data = ExecuteQuery(context, sql.ToString());
                List<Dictionary<string, object>> cos = ExecuteQuery(context, "exec sp_GenericProjectCosWithName -1");

                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();

                Dictionary<string, object> copyDataColumn = new Dictionary<string, object>(DataColumn);
                copyDataColumn.Remove("c_prog");
                csv.Append(string.Join("^$&", copyDataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    //clean html tags
                    row["PV_DS_DESCRIZIONE"] = row["PV_DS_DESCRIZIONE"].ToString().Replace("&amp;nbsp;", "").Replace("&nbsp;", " ").Replace("</p><p>", " ").Replace("&rsquo;", "’").Replace("&rsquo;", "’").Replace("&ugrave;", "ù").Replace("&ugrave", "ù").Replace("&ograve;", "ò").Replace("&ograve", "ò").Replace("&igrave;", "ì").Replace("&igrave", "ì").Replace("&eacute;", "é").Replace("&eacute", "é").Replace("&egrave;", "è").Replace("egrave", "è").Replace("&agrave;", "à").Replace("&agrave", "à").Replace("ugrave;", "ù").Replace("ugrave", "ù").Replace("ograve;", "ò").Replace("ograve", "ò").Replace("igrave;", "ì").Replace("igrave", "ì").Replace("eacute;", "é").Replace("eacute", "é").Replace("egrave;", "è").Replace("egrave", "è").Replace("agrave;", "à").Replace("agrave", "à").Replace("/ppbr/pp", " ").Replace("/ppbr/p", " ").Replace("/pp", " "); ;
                    row["PV_DS_DESCRIZIONE"] = Regex.Replace(row["PV_DS_DESCRIZIONE"].ToString(), "<.*?>|&.*?;", string.Empty);
                    row["PV_S_NOTE"] = row["PV_S_NOTE"].ToString().Replace(";&lt;/p&gt;", "").Replace("&lt;/p&gt;", "").Replace(";&lt;p&gt;", "").Replace("&lt;p&gt;", "").Replace("&lt;p&gt;", " ").Replace("&lt;/p&gt;", "").Replace("&amp;nbsp;", "").Replace("&nbsp;", "").Replace("&quot;", "").Replace("&apos;", "'").Replace("&#39;", "'").Replace("&lt;br&gt;", " ");
                    row["PV_S_NOTE"] = Regex.Replace(row["PV_S_NOTE"].ToString(), "<.*?>|&.*?;", string.Empty);
                    row["PV_DS_DESCRIZIONE"] = row["PV_DS_DESCRIZIONE"].ToString().Replace("^", " ").Replace("\t", "").Replace("\n", "").Replace("&amp;nbsp;", "").Replace("&nbsp;", "").Replace("</p><p>", "");
                    row["PV_S_NOTE"] = row["PV_S_NOTE"].ToString().Replace("^", " ").Replace("\t", "").Replace("\n", "").Replace("&ugrave;", "ù").Replace("&ugrave", "ù").Replace("&ograve;", "ò").Replace("&ograve", "ò").Replace("&igrave;", "ì").Replace("&igrave", "ì").Replace("&eacute;", "é").Replace("&eacute", "é").Replace("&egrave;", "è").Replace("egrave", "è").Replace("&agrave;", "à").Replace("&agrave", "à").Replace("ugrave;", "ù").Replace("ugrave", "ù").Replace("ograve;", "ò").Replace("ograve", "ò").Replace("igrave;", "ì").Replace("igrave", "ì").Replace("eacute;", "é").Replace("eacute", "é").Replace("egrave;", "è").Replace("egrave", "è").Replace("agrave;", "à").Replace("agrave", "à").Replace("/ppbr/pp", " ").Replace("/ppbr/p", " ").Replace("/pp", " ");
                    //end clean
                    var dict = cos.Where(x => x["ProjectId"].ToString() == row["c_prog"].ToString()).ToList();
                    if (dict.Count > 0)
                    {

                        if (dict[0].ContainsKey("Società"))
                        {
                            string societa = dict[0]["Società"].ToString();
                            if (societa.Length > 2)
                            {
                                string[] _societa = societa.Split('-');
                                row["PV_ID_COMMITTENTE_1_LIV_COD"] = _societa[0].Trim();
                                row["PV_DS_COMMITTENTE_1_LIVELLO"] = _societa[1].Trim();
                            }
                        }

                        if (dict[0].ContainsKey("Area"))
                        {
                            string area = dict[0]["Area"].ToString();
                            if (area.Length > 2)
                            {
                                string[] _area = area.Split('-');
                                row["PV_ID_COMMITTENTE_2_LIV_COD"] = _area[0].Trim();
                                row["PV_DS_COMMITTENTE_2_LIVELLO"] = _area[1].Trim();

                            }
                        }

                        if (dict[0].ContainsKey("Servizio"))
                        {
                            string Servizio = dict[0]["Servizio"].ToString();
                            if (Servizio.Length > 2)
                            {
                                string[] _Servizio = Servizio.Split('-');

                                row["PV_ID_COMMITTENTE_3_LIV_COD"] = _Servizio[0].Trim();
                                row["PV_DS_COMMITTENTE_3_LIVELLO"] = _Servizio[1].Trim();

                            }
                        }
                        if (dict[0].ContainsKey("Ufficio"))
                        {
                            string Ufficio = dict[0]["Ufficio"].ToString();
                            if (Ufficio.Length > 2)
                            {
                                string[] _Ufficio = Ufficio.Split('-');
                                row["PV_ID_COMMITTENTE_4_LIV_COD"] = _Ufficio[0].Trim();
                                row["PV_DS_COMMITTENTE_4_LIVELLO"] = _Ufficio[1].Trim();
                            }
                        }
                        if (dict[0].ContainsKey("Team"))
                        {
                            string Team = dict[0]["Team"].ToString();
                            if (Team.Length > 2)
                            {
                                string[] _Team = Team.Split('-');
                                row["PV_ID_COMMITTENTE_5_LIV_COD"] = _Team[0].Trim();
                                row["PV_DS_COMMITTENTE_5_LIVELLO"] = _Team[1].Trim();
                            }
                        }

                    }
                    row.Remove("c_prog");
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }

                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function ProjectExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function ProjectExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("ProjectExport procedure finishes executing");
                saveLog(userInfoModel, "ProjectExport procedure finishes executing", DateTime.Now);
            }
        }

        public string ProjectActualBudgetExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("ProjectActualBudgetExport procedure starts executing");
            saveLog(userInfoModel, "ProjectActualBudgetExport procedure starts executing", DateTime.Now);

            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '') AS DATA_EXEC, \n");
                sql.Append("       'M'                                             AS TIPO_DATO, \n");
                sql.Append("       CASE \n");
                sql.Append("         WHEN b.[year] IS NULL THEN '' \n");
                //sql.Append("         ELSE CONVERT(VARCHAR, b.[year]) + '/' \n");
                //sql.Append("              + CONVERT(VARCHAR, b.[month]) \n");
                sql.Append("        ELSE ((b.[year]) * 100) + b.[month] \n");
                sql.Append("       END                                             AS ANNO_MESE, \n");
                sql.Append("       x.obj_code                                      AS COD_PROGETTO, \n");
                sql.Append("       obs.c_elmnt                                     AS ID_SOGGETTO, \n");
                sql.Append("       obs.s_elmnt                                     AS DESC_SOGGETTO, \n");
                sql.Append("       Isnull(b.budgetcode, '')                        AS COD_VOCE, \n");
                sql.Append("       Isnull(b.budgettitle, '')                       AS DESC_VOCE, \n");
                sql.Append("       Isnull(b.budget, 0)  AS VALORE \n");
                sql.Append("FROM   bpm_objects x \n");
                sql.Append("       Inner JOIN (SELECT d.s_classe                     AS budgettitle, \n");
                sql.Append("                         c.proj_id, \n");
                sql.Append("                         Isnull(Month(b.startdate), '') AS [month], \n");
                sql.Append("                         Isnull(Year(b.startdate), '')  AS [year], \n");
                sql.Append("                         Isnull(b.element_value, 0)     AS budget, \n");
                sql.Append("                         c.element_id                   AS budgetcode \n");
                sql.Append("                  FROM   cost_rev_asso_task c \n");
                sql.Append("                         LEFT JOIN Tab_Cost_Actual_Phasing b \n");
                sql.Append("                                ON b.ass_id = c.ass_id \n");
                sql.Append("                         INNER JOIN classe_di_costo_t043 d \n");
                sql.Append("                                 ON d.c_classe = c.element_id) b \n");
                sql.Append("              ON b.proj_id = x.c_prog and b.budget <> 0 and b.[year] = " + DateTime.Now.Year + "  \n");
                sql.Append("       INNER JOIN (SELECT c_prog, \n");
                sql.Append("                          stru.c_elmnt, \n");
                sql.Append("                          stru.s_elmnt \n");
                sql.Append("                   FROM   ascn_obs_prog obs \n");
                sql.Append("                          INNER JOIN stru_sist_t094 stru \n");
                sql.Append("                                  ON stru.p_elmnt = obs.p_elmnt \n");
                sql.Append("                                     AND stru.c_stru = 'ORG' \n");
                sql.Append("                   WHERE  obs.c_stru = 'ORG') obs \n");
                sql.Append("               ON obs.c_prog = x.c_prog \n");
                sql.Append("WHERE  x.f_id = 10");


                Data = ExecuteQuery(context, sql.ToString());
                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();
                csv.Append(string.Join("^$&", DataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function ProjectActualBudgetExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function ProjectActualBudgetExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("ProjectActualBudgetExport procedure finishes executing");
                saveLog(userInfoModel, "ProjectActualBudgetExport procedure finishes executing", DateTime.Now);
            }
        }

        public string ProjectBudgetExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("ProjectBudgetExport procedure starts executing");
            saveLog(userInfoModel, "ProjectBudgetExport procedure starts executing", DateTime.Now);

            try
            {
                //StringBuilder sql = new StringBuilder();
                //sql.Append("SELECT Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '') AS DATA_EXEC, \n");
                //sql.Append("       'M'                                             AS TIPO_DATO, \n");
                //sql.Append("       CASE \n");
                //sql.Append("         WHEN gh.[year] IS NULL OR gh.[year]=0 THEN ((gh.[yearforecast]) * 100) + gh.[monthforecast]  \n");
                ////sql.Append("         ELSE CONVERT(VARCHAR, b.[year]) + '/' \n");
                ////sql.Append("              + CONVERT(VARCHAR, b.[month]) \n");
                //sql.Append("     ELSE    ((gh.[year]) * 100) + gh.[month] \n");
                //sql.Append("       END                                             AS ANNO_MESE, \n");
                //sql.Append("       x.obj_code                                      AS COD_PROGETTO, \n");
                //sql.Append("       obs.c_elmnt                                     AS ID_SOGGETTO, \n");
                //sql.Append("       obs.s_elmnt                                     AS DESC_SOGGETTO, \n");
                //sql.Append("       Isnull(gh.budgetcode, '')                        AS COD_VOCE, \n");
                //sql.Append("       Isnull(gh.budgettitle, '')                       AS DESC_VOCE, \n");
                //sql.Append("       Isnull(gh.budget, 0) AS VALOREBUDGET, \n");
                //sql.Append("       Isnull(gh.Forecast, 0) AS VALOREFORECAST \n");
                //sql.Append("FROM   bpm_objects x \n");
                //sql.Append("       Inner JOIN (SELECT d.s_classe                     AS budgettitle, \n");
                //sql.Append("                         c.proj_id, \n");
                //sql.Append("                         Isnull(Month(b.startdate), '') AS [month], \n");
                //sql.Append("                         Isnull(Year(b.startdate), '')  AS [year], \n");
                //sql.Append("                         Isnull(Month(p.startdate), '') AS [monthforecast], \n");
                //sql.Append("                         Isnull(Year(p.startdate), '')  AS [yearforecast], \n");
                //sql.Append("                         Isnull(b.element_value, 0)     AS budget, \n");
                //sql.Append("                         Isnull(p.element_value, 0)     AS Forecast, \n");
                //sql.Append("                         c.element_id                   AS budgetcode \n");
                //sql.Append("                  FROM   cost_rev_asso_task c \n");
                //sql.Append("                         LEFT JOIN Tab_Cost_Work_Phasing p \n");
                //sql.Append("                                ON p.ass_id = c.ass_id and Isnull(p.element_value, 0) > 0 and YEAR(p.[startdate]) between " + DateTime.Now.Year + " and " + (DateTime.Now.Year + 2) + " \n");
                //sql.Append("                         LEFT JOIN tab_cost_baseline_phasing b \n");
                //sql.Append("                                ON b.ass_id = c.ass_id and Isnull(b.element_value, 0) > 0 and YEAR(b.[startdate]) between " + DateTime.Now.Year + " and " + (DateTime.Now.Year + 2) + " \n");
                //sql.Append("                         INNER JOIN classe_di_costo_t043 d \n");
                //sql.Append("                                 ON d.c_classe = c.element_id) gh \n");
                //sql.Append("              ON gh.proj_id = x.c_prog  and ( gh.Forecast > 0 OR gh.budget >0) \n");
                //sql.Append("       INNER JOIN (SELECT c_prog, \n");
                //sql.Append("                          stru.c_elmnt, \n");
                //sql.Append("                          stru.s_elmnt \n");
                //sql.Append("                   FROM   ascn_obs_prog obs \n");
                //sql.Append("                          INNER JOIN stru_sist_t094 stru \n");
                //sql.Append("                                  ON stru.p_elmnt = obs.p_elmnt \n");
                //sql.Append("                                     AND stru.c_stru = 'ORG' \n");
                //sql.Append("                   WHERE  obs.c_stru = 'ORG') obs \n");
                //sql.Append("               ON obs.c_prog = x.c_prog \n");
                //sql.Append("WHERE  x.f_id = 10");

                string strQuery = @" 
                    SELECT * from (
                    SELECT Isnull(CONVERT(VARCHAR, Getutcdate(), 103), '') AS DATA_EXEC, 
                           'M'                                             AS TIPO_DATO, 
     
	                     ((gh.[yearforecast]) * 100) + gh.[monthforecast] AS ANNO_MESE,
                           x.obj_code                                      AS COD_PROGETTO, 
                           obs.c_elmnt                                     AS ID_SOGGETTO, 
                           obs.s_elmnt                                     AS DESC_SOGGETTO, 
                           Isnull(gh.budgetcode, '')                        AS COD_VOCE, 
                           Isnull(gh.budgettitle, '')                       AS DESC_VOCE, 
                           Isnull(gh.budget, 0) AS VALOREBUDGET, 
                           Isnull(gh.Forecast, 0) AS VALOREFORECAST  
                    FROM   bpm_objects x 
                           Inner JOIN (SELECT d.s_classe                     AS budgettitle, 
                                             CR.proj_id, 
                                             Isnull(Month(CR.BDStartDate), '') AS [month], 
                                             Isnull(Year(CR.BDFinishDate), '')  AS [year], 
                                             Isnull(Month(CR.startdate), '') AS [monthforecast], 
                                             Isnull(Year(CR.startdate), '')  AS [yearforecast], 
                                             Isnull(CR.BDelement_value, 0)     AS budget, 
                                             Isnull(CR.element_value, 0)     AS Forecast, 
                                             CR.element_id                   AS budgetcode ,
						                      CR.BDStartDate AS [BDStartDate],
						                      CR.ASS_ID
                                      FROM   
				                      (select C.ASS_ID, C.proj_id,C.element_id,BD.STARTDATE as BDStartDate,BD.FINISHDATE as BDFinishDate,BD.ELEMENT_VALUE as BDELEMENT_VALUE,p.FINISHDATE,p.STARTDATE,p.ELEMENT_VALUE
				                      from Cost_Rev_Asso_Task C
				                    INNER JOIN Tab_Cost_Work_Phasing p ON p.ass_id = c.ass_id and Isnull(p.element_value, 0) > 0 and YEAR(p.[startdate]) between @@StartYear and @@FinishYear 
				                    LEFT JOIN (SELECT * FROM tab_cost_baseline_phasing WHERE Isnull(element_value, 0) > 0 and YEAR([startdate]) between @@StartYear and @@FinishYear)BD
				                    ON BD.ASS_ID=C.ASS_ID AND BD.STARTDATE=p.STARTDATE and BD.FINISHDATE=p.FINISHDATE 
				                    ) CR
                         
                                             INNER JOIN classe_di_costo_t043 d 
                                                     ON d.c_classe = CR.element_id) gh 
                                  ON gh.proj_id = x.c_prog   
                           INNER JOIN (SELECT c_prog, 
                                              stru.c_elmnt, 
                                              stru.s_elmnt 
                                       FROM   ascn_obs_prog obs 
                                              INNER JOIN stru_sist_t094 stru 
                                                      ON stru.p_elmnt = obs.p_elmnt 
                                                         AND stru.c_stru = 'ORG' 
                                       WHERE  obs.c_stru = 'ORG') obs 
                                   ON obs.c_prog = x.c_prog 
                    WHERE  x.f_id = 10  )p ".Replace("@@StartYear", DateTime.Now.Year.ToString()).Replace("@@FinishYear", (DateTime.Now.Year + 2).ToString());
                
                Data = ExecuteQuery(context, strQuery.ToString());
                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();
                csv.Append(string.Join("^$&", DataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }

                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function ProjectBudgetExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function ProjectBudgetExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("ProjectBudgetExport procedure finishes executing");
                saveLog(userInfoModel, "ProjectBudgetExport procedure finishes executing", DateTime.Now);
            }
        }

        public string TimesheetExport(UserInfoModel userInfoModel, PODBContext context)
        {
            List<Dictionary<string, object>> Data = null;
            Helper.WriteInformation("TimesheetExport procedure starts executing");
            saveLog(userInfoModel, "TimesheetExport procedure starts executing", DateTime.Now);

            try
            {
                StringBuilder sql = new StringBuilder();
                sql.Append("SELECT * \n");
                sql.Append("FROM   (SELECT * \n");
                sql.Append("        FROM   (SELECT CONVERT(VARCHAR, Getdate(), 103) AS DATA_RIF, \n");
                sql.Append("                       CONVERT(VARCHAR, h.data, 103)    AS PERIOD, \n");
                sql.Append("                       m.task_uid                       AS MACROPHASEID, \n");
                sql.Append("                       CONVERT(VARCHAR, obs.s_elmnt)    AS SUBSYSTEM, \n");
                sql.Append("                       b.obj_code                       AS [DBID], \n");
                sql.Append("                       b.obj_title                      AS PROJECT, \n");
                sql.Append("             CASE \n");
                sql.Append("                        WHEN m.task_outline_level < 2 THEN '' \n");
                sql.Append("                        WHEN m.task_outline_level = 2 THEN (select Top 1 TASK_NAME from MSP_TASKS WITH(NOLOCK) where PROJ_ID = m.PROJ_ID and TASK_UID = m.TASK_UID) \n");
                sql.Append("                        WHEN task_outline_level = 3 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=2 THEN ( select Top 1  TASK_NAME from MSP_TASKS WITH(NOLOCK)  where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        WHEN m.task_outline_level = 4  AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=3 THEN ( select Top 1  TASK_NAME from MSP_TASKS WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                            Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        WHEN m.task_outline_level = 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=4 THEN ( select Top 1  TASK_NAME from MSP_TASKS WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        WHEN m.task_outline_level > 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=5 THEN  ( select Top 1  TASK_NAME from MSP_TASKS WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                            Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                         END                             AS Macrophase, \n");
                sql.Append("            CASE \n");
                sql.Append("                         WHEN m.task_outline_level < 3 THEN '' \n");
                sql.Append("                         WHEN m.task_outline_level = 3 THEN (select Top 1  TASK_NAME from MSP_TASKS WITH(NOLOCK) where PROJ_ID = m.PROJ_ID and TASK_UID = m.TASK_UID) \n");
                sql.Append("                         WHEN m.task_outline_level = 4  AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=3 THEN ( select Top 1 TASK_NAME from MSP_TASKS WITH(NOLOCK)  where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                         Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                         WHEN m.task_outline_level = 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=4  THEN ( select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        WHEN m.task_outline_level > 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=5 THEN ( select  Top 1 TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        END                             AS PHASE, \n");
                sql.Append("            CASE \n");
                sql.Append("                         WHEN m.task_outline_level < 4 THEN '' \n");
                sql.Append("                         WHEN m.task_outline_level = 4  AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=3 THEN (select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where PROJ_ID = m.PROJ_ID and TASK_UID = m.TASK_UID) \n");
                sql.Append("                         WHEN m.task_outline_level = 5  AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=4  THEN ( select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("                        WHEN m.task_outline_level > 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=5 THEN ( select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                        Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("            END                                             AS TASK, \n");
                sql.Append("           CASE \n");
                sql.Append("                         WHEN m.task_outline_level < 5 THEN '' \n");
                sql.Append("                          WHEN m.task_outline_level = 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=4 THEN (select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where PROJ_ID = m.PROJ_ID and TASK_UID = m.TASK_UID) \n");
                sql.Append("                         WHEN m.task_outline_level > 5 AND LEN(m.TASK_OUTLINE_NUM) - LEN(REPLACE(m.TASK_OUTLINE_NUM , '.', '')) >=5 THEN ( select Top 1  TASK_NAME from MSP_TASKS tem WITH(NOLOCK) where ISNULL(CAST(TASK_WBS AS varchar(2000)) ,'')= \n");
                sql.Append("                                Left(Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''), \n");
                sql.Append("                        charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''),charindex('.',  Isnull(CAST(m.TASK_WBS AS varchar(2000)) ,''))+1)+1)+1)+1)-1) \n");
                sql.Append("                        and Proj_ID = m.PROJ_ID) \n");
                sql.Append("            END                                             AS SUBTASK, \n");
                sql.Append("                       r.s_name                         AS USERNAME, \n");
                sql.Append("                       Isnull(h.[hours], 0)             AS [HOURS], \n");
                sql.Append("                       CASE \n");
                sql.Append("                         WHEN h.is_billable = 1 THEN 'Y' \n");
                sql.Append("                         ELSE 'N' \n");
                sql.Append("                       END                              AS BILLABLE, \n");
                sql.Append("                       Isnull(usercode.c_user, '')      AS USERCODE, \n");
                sql.Append("                       Isnull(l.s_location, '')         LOCATION, \n");
                sql.Append("                       Isnull(d.s_depart, '')           AS GRUPPORESP, \n");
                sql.Append("                       prtype.menu_name                 AS TIPOPROGETTO, \n");
                sql.Append("                       CASE \n");
                sql.Append("                         WHEN r.c_ietype = 0 THEN 'Internal' \n");
                sql.Append("                         ELSE 'External' \n");
                sql.Append("                       END                              AS TIPOLOGIA_RISORSA, \n");
                sql.Append("                         CASE When t.TIPO != 'P' then '' when msp.WLT_ID is not null and  msp.WLT_ID != 0 and msp.WLT_ID != -1 then (select S_WORK_TYPE from TAB_WORK_TYPE WITH(NOLOCK) where WORK_TYPE_ID = msp.WLT_ID)  \n");
                sql.Append("                         WHEN t.WORK_TYPE_ID is not null then wt.S_WORK_TYPE \n");
                sql.Append("                         WHEN t.ovt = 0 THEN 'Standard Work' \n");
                sql.Append("                         ELSE 'Overtime' \n");
                sql.Append("                       END                              AS WORK_TYPE \n");
                sql.Append("                FROM   tab_tse t WITH(NOLOCK) \n");
                sql.Append("                       left join MSP_TASK_ASSIGNMENT_USERS msp WITH(NOLOCK) on msp.ASS_ID = t.ASS_ID_WT  \n");
                sql.Append("                       INNER JOIN tab_ts ts WITH(NOLOCK) \n");
                sql.Append("                               ON t.c_ts = ts.c_ts \n");
                sql.Append("                        INNER JOIN  MSP_TASKS m WITH(NOLOCK) \n");
                sql.Append("                               ON t.c_prog = m.proj_id \n");
                sql.Append("                                  AND t.id_task = m.task_uid \n");
                sql.Append("                       INNER JOIN bpm_objects b WITH(NOLOCK) \n");
                sql.Append("                               ON b.c_prog = t.c_prog \n");
                sql.Append("                                  AND b.f_id = 10 \n");
                sql.Append("                       INNER JOIN (SELECT c_prog, \n");
                sql.Append("                                          stru.c_elmnt, \n");
                sql.Append("                                          stru.s_elmnt \n");
                sql.Append("                                   FROM   ascn_obs_prog obs WITH(NOLOCK) \n");
                sql.Append("                                          INNER JOIN stru_sist_t094 stru WITH(NOLOCK) \n");
                sql.Append("                                                  ON stru.p_elmnt = obs.p_elmnt \n");
                sql.Append("                                                     AND stru.c_stru = 'ORG' \n");
                sql.Append("                                   WHERE  obs.c_stru = 'ORG') obs \n");
                sql.Append("                               ON obs.c_prog = t.c_prog \n");
                sql.Append("                       LEFT JOIN tab_dip r WITH(NOLOCK) \n");
                sql.Append("                              ON r.c_dip = ts.c_dip \n");
                sql.Append("                       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                                         c_dip \n");
                sql.Append("                                  FROM   uten_t017 WITH(NOLOCK)) usercode \n");
                sql.Append("                              ON usercode.c_dip = ts.c_dip \n");
                sql.Append("                       LEFT JOIN tab_locations l WITH(NOLOCK) \n");
                sql.Append("                              ON l.c_location = r.c_location \n");
                sql.Append("                       LEFT JOIN tab_work_type wt WITH(NOLOCK) \n");
                sql.Append("                              ON wt.work_type_id = t.work_type_id \n");
                sql.Append("                       LEFT JOIN tab_departments d WITH(NOLOCK) \n");
                sql.Append("                              ON d.c_depart = r.c_depart \n");
                sql.Append("                       LEFT JOIN (SELECT menu_name, \n");
                sql.Append("                                         f_type, \n");
                sql.Append("                                         f_id \n");
                sql.Append("                                  FROM   bpm_menu WITH(NOLOCK) \n");
                sql.Append("                                  WHERE  f_id = 10) prtype \n");
                sql.Append("                              ON prtype.f_type = b.f_type \n");
                sql.Append("                                 AND prtype.f_id = 10 \n");
                sql.Append("                       LEFT JOIN (SELECT Sum(ore) AS [hours], \n");
                sql.Append("                                         t.c_ts, \n");
                sql.Append("                                         t.pro, \n");
                sql.Append("                                         is_billable, \n");
                sql.Append("                                         data \n");
                sql.Append("                                  FROM   tab_tse t WITH(NOLOCK) \n");
                sql.Append("                                         INNER JOIN tab_hours h WITH(NOLOCK) \n");
                sql.Append("                                                 ON t.c_ts = h.c_ts \n");
                sql.Append("                                                    AND h.pro = t.pro \n");
                sql.Append("                                  GROUP  BY t.c_ts, \n");
                sql.Append("                                            t.pro, \n");
                sql.Append("                                            is_billable, \n");
                sql.Append("                                            data) h \n");
                sql.Append("                              ON h.c_ts = t.c_ts \n");
                sql.Append("                                 AND h.pro = t.pro) x \n");
                sql.Append("        WHERE  period IS NOT NULL \n");
                sql.Append("        UNION \n");
                sql.Append("        SELECT * \n");
                sql.Append("        FROM   (SELECT CONVERT(VARCHAR, Getdate(), 103) AS DATA_RIF, \n");
                sql.Append("                       CONVERT(VARCHAR, h.data, 103)    AS PERIOD, \n");
                sql.Append("                       ''                               AS MACROPHASEID, \n");
                sql.Append("                       CONVERT(VARCHAR, '')             AS SUBSYSTEM, \n");
                sql.Append("                       ''                               AS [DBID], \n");
                sql.Append("                       ''                               AS PROJECT, \n");
                sql.Append("                       N.s_att                          AS MACROPHASE, \n");
                sql.Append("                       ''                               AS PHASE, \n");
                sql.Append("                       ''                               AS TASK, \n");
                sql.Append("                       ''                               AS SUBTASK, \n");
                sql.Append("                       r.s_name                         AS USERNAME, \n");
                sql.Append("                       Isnull(h.[hours], 0)             AS [HOURS], \n");
                sql.Append("                       CASE \n");
                sql.Append("                         WHEN h.is_billable = 1 THEN 'Y' \n");
                sql.Append("                         ELSE 'N' \n");
                sql.Append("                       END                              AS BILLABLE, \n");
                sql.Append("                       Isnull(usercode.c_user, '')      AS USERCODE, \n");
                sql.Append("                       Isnull(l.s_location, '')         LOCATION, \n");
                sql.Append("                       Isnull(d.s_depart, '')           AS GRUPPORESP, \n");
                sql.Append("                       ''                               AS TIPOPROGETTO, \n");
                sql.Append("                       CASE \n");
                sql.Append("                         WHEN r.c_ietype = 0 THEN 'Internal' \n");
                sql.Append("                         ELSE 'External' \n");
                sql.Append("                       END                              AS TIPOLOGIA_RISORSA, \n");
                sql.Append("                         CASE When t.TIPO != 'P' then '' when msp.WLT_ID is not null and  msp.WLT_ID != 0 and msp.WLT_ID != -1 then (select S_WORK_TYPE from TAB_WORK_TYPE WITH(NOLOCK) where WORK_TYPE_ID = msp.WLT_ID)  \n");
                sql.Append("                         WHEN t.WORK_TYPE_ID is not null then wt.S_WORK_TYPE \n");
                sql.Append("                         WHEN t.ovt = 0 THEN 'Standard Work' \n");
                sql.Append("                         ELSE 'Overtime' \n");
                sql.Append("                       END                              AS WORK_TYPE \n");
                sql.Append("                FROM   tab_tse t WITH(NOLOCK) \n");
                sql.Append("                       left join MSP_TASK_ASSIGNMENT_USERS msp WITH(NOLOCK) on msp.ASS_ID = t.ASS_ID_WT  \n");
                sql.Append("                       INNER JOIN tab_ts ts WITH(NOLOCK) \n");
                sql.Append("                               ON t.c_ts = ts.c_ts \n");
                sql.Append("                                  AND T.tipo = 'N' \n");
                sql.Append("                       INNER JOIN tab_non_lav N WITH(NOLOCK) \n");
                sql.Append("                               ON N.c_att = t.c_prog \n");
                sql.Append("                       LEFT JOIN tab_dip r WITH(NOLOCK) \n");
                sql.Append("                              ON r.c_dip = ts.c_dip \n");
                sql.Append("                       LEFT JOIN (SELECT c_user, \n");
                sql.Append("                                         c_dip \n");
                sql.Append("                                  FROM   uten_t017 WITH(NOLOCK)) usercode \n");
                sql.Append("                              ON usercode.c_dip = ts.c_dip \n");
                sql.Append("                       LEFT JOIN tab_locations l WITH(NOLOCK) \n");
                sql.Append("                              ON l.c_location = r.c_location \n");
                sql.Append("                       LEFT JOIN tab_work_type wt WITH(NOLOCK) \n");
                sql.Append("                              ON wt.work_type_id = t.work_type_id \n");
                sql.Append("                       LEFT JOIN tab_departments d WITH(NOLOCK) \n");
                sql.Append("                              ON d.c_depart = r.c_depart \n");
                sql.Append("                       LEFT JOIN (SELECT Sum(ore) AS [hours], \n");
                sql.Append("                                         t.c_ts, \n");
                sql.Append("                                         t.pro, \n");
                sql.Append("                                         is_billable, \n");
                sql.Append("                                         data \n");
                sql.Append("                                  FROM   tab_tse t WITH(NOLOCK) \n");
                sql.Append("                                         INNER JOIN tab_hours h WITH(NOLOCK) \n");
                sql.Append("                                                 ON t.c_ts = h.c_ts \n");
                sql.Append("                                                    AND h.pro = t.pro \n");
                sql.Append("                                  GROUP  BY t.c_ts, \n");
                sql.Append("                                            t.pro, \n");
                sql.Append("                                            is_billable, \n");
                sql.Append("                                            data) h \n");
                sql.Append("                              ON h.c_ts = t.c_ts \n");
                sql.Append("                                 AND h.pro = t.pro) x \n");
                sql.Append("        WHERE  period IS NOT NULL) x \n");
                sql.Append("ORDER  BY x.username, \n");
                sql.Append("          x.period, \n");
                sql.Append("          x.project");

                Data = ExecuteQuery(context, sql.ToString(), 180);
                StringBuilder csv = new StringBuilder();
                if (Data.Count == 0)
                {
                    return "";
                }
                Dictionary<string, object> DataColumn = Data.FirstOrDefault();
                csv.Append(string.Join("^$&", DataColumn.Select(x => x.Key).ToArray()));
                csv.Append("\r\n");

                foreach (var row in Data)
                {
                    csv.Append(string.Join("^$&", row.Select(x => x.Value).ToArray()));
                    csv.Append("\r\n");
                }
                return csv.ToString();
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, $"Error in function TimesheetExport: {ex.Message}");
                saveLog(userInfoModel, $"Error in function TimesheetExport: {ex}", DateTime.Now);
                throw;
            }
            finally
            {
                Helper.WriteInformation("TimesheetExport procedure finishes executing");
                saveLog(userInfoModel, "TimesheetExport procedure finishes executing", DateTime.Now);
            }
        }

        public string DictionaryToCSV(List<Dictionary<string, object>> Data)
        {
            string csv = string.Empty;

            Dictionary<string, object> DataColumn = Data.FirstOrDefault();

            foreach (var column in DataColumn)
            {
                if (column.Key.ToString().ToLower() != "c_prog")
                {
                    csv += column.Key + "^$&";
                }

            }
            csv += "\r\n";

            foreach (var row in Data)
            {

                foreach (var item in row)
                {
                    if (item.Key.ToString().ToLower() != "c_prog")
                    {
                        csv += item.Value.ToString() + "^$&";
                    }
                }
                csv += "\r\n";
            }

            return csv;
        }

        public Dictionary<string, object> saveLog(UserInfoModel userInfo, string txt, DateTime createdNow)
        {
            Dictionary<string, object> Data = new Dictionary<string, object>();
            try
            {
                using (var context = GetPODBContextInstance(userInfo.CustomerId))
                {
                    string dateNow = DbUtility.QDTMS(createdNow.ToString());
                    string sqlStr = "Insert into ErrorLogExport(comment, created_date) values(" + DbUtility.QS(txt) + "," + dateNow + ") ";
                    var res = context.Database.ExecuteSqlCommand(sqlStr);
                    if (res != 0)
                    {
                        Data["comment"] = txt;
                        Data["created_date"] = createdNow;
                        return Data;
                    }
                }
            }
            catch (Exception ex)
            {
                Helper.WriteError(ex, ex.Message);
            }

            return null;
        }

        //Added by Sunny singh
        public Dictionary<string, object> sellaRateLog(string txt, DateTime createdNow , string IdNum)
        {


            var ConnectionString = "";
            var ConnectionStringorigional = System.Configuration.ConfigurationManager.ConnectionStrings["CustomConnection"].ConnectionString;
            ConnectionString = PwCrypt.PwCrypt.DeCryptDsnPassword(ConnectionStringorigional);
            string insertQuery = "INSERT INTO Sella_Rate_ErrorLog (ASS_ID, comment,created_date) VALUES (@IdNum,@txt, @createdNow)";

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                try
                {
                     using (SqlCommand command = new SqlCommand(insertQuery, connection))
                    { 

                        command.Parameters.AddWithValue("@IdNum", IdNum);
                        command.Parameters.AddWithValue("@txt", txt);
                        command.Parameters.AddWithValue("@createdNow", createdNow);
                        int rowsAffected = command.ExecuteNonQuery();
                        if (rowsAffected > 0)
                        {
                            Console.WriteLine("Data inserted successfully.");
                        }
                        else
                        {
                            Console.WriteLine("No rows affected. Data insertion failed.");
                        }

                    }



                }
                catch (Exception ex)
                {
                }

                connection.Close();
            }

             
            return null;
        }

        private async void SendEmailsAsync(PODBContext _context, UserInfoModel userInfo, List<Dictionary<string, object>> emailList)
        {
            var context = GetPODBContextInstance(userInfo.CustomerId);
            try
            {
                System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12;
                var SendGridAPIKey = System.Configuration.ConfigurationManager.AppSettings["SendGridAPIKey"];
                var user = System.Configuration.ConfigurationManager.AppSettings["botUserId"];
                var client = new SendGridClient(SendGridAPIKey);
                string templatepath = System.Web.Hosting.HostingEnvironment.MapPath("~/Templates/Generic.html");
                long maxId = context.Database.SqlQuery<long>("select isnull(max(IterationId),0) from CesPitiEmailImportLog(noLock)").SingleOrDefault();

                foreach (var val in emailList)
                {
                    try
                    {
                        string template = File.ReadAllText(templatepath);
                        var msg = new SendGridMessage();
                        template = template.Replace("[Initiative Code]", val["ProjectCode"].ToString());
                        template = template.Replace("[Initiative Title]", val["ProjectTitle"].ToString());
                        msg.SetFrom(new EmailAddress(user, user));
                        //msg.AddTo(new EmailAddress(val["UserEmail"].ToString()));
                        msg.AddTo(new EmailAddress("f.ruggeri@uppwise.com"));
                        msg.SetSubject("Uppwise: valutazione Cespiti " + val["ProjectCode"] + " " + val["ProjectTitle"]);
                        msg.AddContent(MimeType.Html, template);
                        var response = await client.SendEmailAsync(msg);
                        await CreateLogEntryAsync("successfully", userInfo, Convert.ToString(val["ProjectCode"]), maxId);
                    }
                    catch (Exception ex)
                    {
                        await CreateLogEntryAsync("successfully", userInfo, Convert.ToString(val["ProjectCode"]), maxId);

                    }

                }
                string Str = "delete from CesPitiEmailImport";
                context.Database.ExecuteSqlCommand(Str);

            }
            catch (Exception ex)
            {
                string Str = "delete from CesPitiEmailImport";
                context.Database.ExecuteSqlCommand(Str);
                throw;
            }


        }

        private async Task CreateLogEntryAsync(string message, UserInfoModel userInfo, string projectCode, long id)
        {
            using (var context = GetPODBContextInstance(userInfo.CustomerId))
            {
                using (var transaction = context.Database.BeginTransaction())
                {
                    try
                    {
                        string sqlStr = "update CesPitiEmailImportLog set EmailSendMsg = " + DbUtility.QS(message) + " where ProjectCode=" + projectCode + " and IterationId =" + id + " ";
                        context.Database.ExecuteSqlCommand(sqlStr);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw ex;
                    }
                }
            }
        }

        //added bu Sunny Singh
        public string SSellaRateMatrixCustomCalculation(long IdNum)
        {

            var ConnectionString = "";
            var ConnectionStringorigional = System.Configuration.ConfigurationManager.ConnectionStrings["CustomConnection"].ConnectionString;
            ConnectionString = PwCrypt.PwCrypt.DeCryptDsnPassword(ConnectionStringorigional);
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                //added by Sunny Singh log  01/02/24
                Helper.WriteInformation("SSellaRateMatrixCustomCalculation procedure starts executing");
                
                try
                {
                    var storedProcedureName = "sp_SellaRatematric_custom";
                    using (SqlCommand command = new SqlCommand(storedProcedureName, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.Parameters.Add(new SqlParameter("@ASS_ID", SqlDbType.BigInt)).Value = IdNum;
                        SqlParameter outputParameter = new SqlParameter("@Result", SqlDbType.VarChar, 50);
                        outputParameter.Direction = ParameterDirection.Output;
                        command.Parameters.Add(outputParameter);
                        command.ExecuteNonQuery();
                        string outputValue = outputParameter.Value.ToString();

                    }



                }
                catch (Exception ex)
                {
                }

                connection.Close();
            }



            return "Sucessfully";
        }
    }
}