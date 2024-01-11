using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using latlog;
using System.Diagnostics;

namespace Roughdb
{
    internal class databaseloader
    {
        public static string InferPrimaryKeyColumnName(DataTable schemaTable)
        {
            var primaryKeyColumn = schemaTable.Columns.Cast<DataColumn>().FirstOrDefault(c => c.Unique);

            return primaryKeyColumn != null ? primaryKeyColumn.ColumnName : null;
        }
        public bool TableExists(string connectionString, string tableName)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}_clone'";

                        int count = (int)command.ExecuteScalar();

                        Latlog.Log(LogLevel.Info, $"Table '{tableName}_clone' exists: {count > 0}");

                        return count > 0;
                    }
                }
            }
            catch (Exception ex)
            {
                Latlog.LogError("TableExists", $"Exception occured {ex.Message}", ex);
                return false;
            }
        }
        public void CreateTable(string connectionString, string tableName, DataTable schemaTable)
        {
            try
            {
                //DataTable schemaTable = LoadSchemaTable(tableName, schemaXmlFilePath);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;

                        string createTableCommand = GenerateCreateTableCommand(tableName, schemaTable);

                        command.CommandText = createTableCommand;

                        command.ExecuteNonQuery();
                    }
                }

                Latlog.Log(LogLevel.Info, $"Table '{tableName}' created successfully.");
            }
            catch (Exception ex)
            {
                Latlog.LogError("CreateTable", $"Exception occured {ex.Message}", ex);
                //
                //Latlog.Log(LogLevel.Info,$"Exception Stack Trace: {ex.StackTrace}");
            }

        }
        public static string GenerateCreateTableCommand(string tableName, DataTable schemaTable)
        {
            try
            {
                StringBuilder createTableCommand = new StringBuilder();
                createTableCommand.Append($"CREATE TABLE {tableName}_clone (");


                List<string> columnDefinitions = new List<string>();
                List<string> constraints = new List<string>();

                foreach (DataColumn column in schemaTable.Columns)
                {
                    string columnName = column.ColumnName;
                    Type dataType = column.DataType;

                    // Build the column definition using the updated mapping
                    string columnDefinition = $"{columnName} {MapDataTypeToSqlType(dataType, column.Unique)}";

                    // Add debug statement to inspect the column definition
                    Console.WriteLine($"Column Definition: {columnDefinition}");
                    // Check for constraints
                    if (column.Unique)
                    {
                        Console.WriteLine("Primary Key is True");
                        constraints.Add($"CONSTRAINT PK_{tableName}_{columnName} PRIMARY KEY ({columnName})");
                        Console.WriteLine($"Added PK constraint for column: {columnName}");
                    }
                    else if (Convert.ToBoolean(column.ExtendedProperties["IS_FOREIGN_KEY"]))
                    {
                        string foreignKeyTable = column.ExtendedProperties["FOREIGN_KEY_TABLE"].ToString();
                        string foreignKeyColumn = column.ExtendedProperties["FOREIGN_KEY_COLUMN"].ToString();
                        constraints.Add($"CONSTRAINT FK_{tableName}_{columnName} FOREIGN KEY ({columnName}) REFERENCES {foreignKeyTable}({foreignKeyColumn})");
                        Console.WriteLine($"Added FK constraint for column: {columnName}");
                    }

                    columnDefinitions.Add(columnDefinition);
                }
                columnDefinitions.Add("TimestampColumn timestamp");

                Console.WriteLine($"Column Definitions: {string.Join(", ", columnDefinitions)}");

                createTableCommand.Append(string.Join(", ", columnDefinitions));

                // Append constraints only if there are any
                if (constraints.Count > 0)
                {
                    Console.WriteLine($"Constraints: {string.Join(", ", constraints)}");
                    createTableCommand.Append(", " + string.Join(", ", constraints));
                }

                createTableCommand.Append(");");

                Latlog.Log(LogLevel.Info, $"SQL Command: {createTableCommand}");

                return createTableCommand.ToString();
            }
            catch (Exception ex)
            {
                Latlog.LogError("GenerateCreateTableCommand", $"Exception occured {ex.Message}", ex);
                throw; // Rethrow the exception after logging
            }
        }

        public static string MapDataTypeToSqlType(Type dataType, bool isPrimaryKey = false)
        {
            if (isPrimaryKey)
            {
                // Handle special cases for primary key data types
                if (dataType == typeof(int))
                {
                    return "INT";
                }
                // Add more cases for other primary key data types as needed
            }

            // Generic mapping for other data types
            switch (Type.GetTypeCode(dataType))
            {
                case TypeCode.Int32:
                    return "INT";
                case TypeCode.String:
                    return "VARCHAR(MAX)";
                case TypeCode.DateTime:
                    return "DATE";
                case TypeCode.Char:
                    return "CHAR(1)";
                // Add more cases for other data types as needed
                default:
                    return "VARCHAR(MAX)";
            }
        }
        public void InsertDataInSql(string connectionString,  DataTable SchemaTable, string targetTableName, string primarykeycolumn , DataTable dataTable)
        {
            Latlog.Log(LogLevel.Info ,"Entered Insertdatainsql function..");
            // Check and create stored procedure
            CreateStoredProcedure(connectionString, targetTableName , primarykeycolumn, SchemaTable , dataTable);

        }
        private static bool ProcedureExists(SqlConnection connection, string procedureName)
        {
            using (SqlCommand command = new SqlCommand("SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = @ProcedureName", connection))
            {
                command.Parameters.AddWithValue("@ProcedureName", procedureName);
                return command.ExecuteScalar() != null;
            }
        }

        private static string GenerateCustomTableType(DataTable schemaTable)
        {
            try
            {
                StringBuilder typeBuilder = new StringBuilder();
                typeBuilder.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.types WHERE name = 'CustomTableType') ");
                typeBuilder.AppendLine("BEGIN");
                typeBuilder.AppendLine("    CREATE TYPE dbo.CustomTableType AS TABLE (");

                List<string> columnDefinitions = new List<string>();

                foreach (DataColumn column in schemaTable.Columns)
                {
                    string columnName = column.ColumnName;
                    Type dataType = column.DataType;
                    bool isNullable = !column.Unique && Convert.ToBoolean(column.ExtendedProperties["IS_NULLABLE"]);

                    // Build the column definition using the existing mapping
                    string columnDefinition = $"{columnName} {MapDataTypeToSqlType(dataType, isNullable)}";

                    columnDefinitions.Add(columnDefinition);
                }

                typeBuilder.AppendLine(string.Join(", ", columnDefinitions));
                typeBuilder.AppendLine(");");
                typeBuilder.AppendLine("END");
                Latlog.Log(LogLevel.Info, $"Custom Table Type is:{typeBuilder}");

                return typeBuilder.ToString();
            }
            catch (Exception ex)
            {
                Latlog.LogError("GenerateCustomTableType", $"Exception occurred: {ex.Message}", ex);
                throw; // Rethrow the exception after logging
            }
        }


        public static void CreateStoredProcedure(string connectionString, string tableName, string primaryKeyColumn, DataTable schemaTable, DataTable dataTable)
        {
            Latlog.Log(LogLevel.Info, "Entered CreateStoredProccedure function..");

            using (SqlConnection con = new SqlConnection(connectionString))
            {
                try
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand();

                    // Check if the stored procedure already exists
                    if (!ProcedureExists(con, $"fullSync_{tableName}_clone"))
                    {
                        // Create the custom table type dynamically
                        string customTableTypeSql = GenerateCustomTableType(schemaTable);

                        // Create the custom table type first
                        cmd.CommandText = customTableTypeSql;
                        cmd.Connection = con;
                        cmd.ExecuteNonQuery();
                        Latlog.Log(LogLevel.Info, "Custom table type created successfully.");

                        // Now create the stored procedure dynamically
                        cmd.CommandText = $@"
                    CREATE PROCEDURE fullSync_{tableName}_clone
                        @tblLog dbo.CustomTableType READONLY
                    AS
                    BEGIN
                        SET NOCOUNT ON;

                        -- Dynamic SQL for inserting data into the target table
                        INSERT INTO dbo.[{tableName}_clone] ({string.Join(", ", schemaTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName))})
                        SELECT * FROM @tblLog;
                    END";

                        cmd.ExecuteNonQuery();
                        Latlog.Log(LogLevel.Info, $"Stored procedure 'fullSync_{tableName}_clone' created successfully.");

                        // Now execute the stored procedure with your data
                        ExecuteStoredProcedure(connectionString, $"fullSync_{tableName}_clone", dataTable);
                    }
                    else
                    {
                        ExecuteStoredProcedure(connectionString, $"fullSync_{tableName}_clone", dataTable);
                        Latlog.Log(LogLevel.Info, $"Stored procedure 'fullSync_{tableName}_clone' already exists.");
                    }
                }
                catch (Exception ex)
                {
                    Latlog.LogError("CreateStoredProcedure", $"Exception occurred: {ex.Message}", ex);
                }
            }
        }


        private static void ExecuteStoredProcedure(string connectionString, string storedProcedureName, DataTable dataTable)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                try
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand
                    {
                        Connection = con,
                        CommandType = CommandType.StoredProcedure,
                        CommandText = storedProcedureName
                    };

                    // Add DataTable parameter to the stored procedure
                    var dataTableParam = new SqlParameter("@tblLog", SqlDbType.Structured)
                    {
                        TypeName = "dbo.CustomTableType", // Update with your actual table type
                        Value = dataTable
                    };
                    cmd.Parameters.Add(dataTableParam);

                    // Execute the stored procedure
                    cmd.ExecuteNonQuery();
                    Latlog.Log(LogLevel.Info, $"Stored procedure '{storedProcedureName}' executed successfully.");
                }
                catch (Exception ex)
                {
                    Latlog.LogError("ExecuteStoredProcedure", $"Exception occurred: {ex.Message}", ex);
                }
            }
        }



        private static string GetPrimaryKeyColumn(string primaryKeyColumn)
        {
            return $"LT.{primaryKeyColumn}";
        }

        private static string GetWhereConditionColumn(DataTable schemaTable, string primaryKeyColumn)
        {
            return $"{GetPrimaryKeyColumn(primaryKeyColumn)} = OT.{primaryKeyColumn}";
        }

    }
}
