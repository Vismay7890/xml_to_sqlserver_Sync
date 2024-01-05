using System;
using System.Configuration;
using System.Data;
using LogLibrary;
using datareader;
class MainDriver
{
    static void Main()
    {
        Logger logger = new Logger();
        Reader dataReader = new Reader();       

        try
        {
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            string tableName = ConfigurationManager.AppSettings["TableName"];
            string schemaXmlFilePath = ConfigurationManager.AppSettings["SchemaFilePath"];
            string dataXmlFilePath = ConfigurationManager.AppSettings["DataFilePath"];

            Logger.Log("Program started.");


            if (!dataReader.TableExists(logger ,connectionString, tableName))
            {
                dataReader.CreateTable(logger,connectionString, tableName, schemaXmlFilePath);
            }

            DataTable schemaTable = dataReader.LoadSchemaTable(logger , tableName ,schemaXmlFilePath);
            DataTable dataTable = dataReader.LoadDataTable(logger,tableName,dataXmlFilePath, schemaTable);
            dataReader.InsertDataInSql(logger,dataTable, connectionString , tableName);
            
            Logger.Log("Program completed successfully.");
        }
        catch (Exception ex)
        {
            Logger.Log($"Exception occurred: {ex.Message}");
        }
    }
}
