using System;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text;
using System.Xml;
using LogLibrary;

namespace datareader {  
class Reader
{
   public  bool TableExists(Logger logger, string connectionString, string tableName)
    {
        try
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'";

                    int count = (int)command.ExecuteScalar();

                    Logger.Log($"Table '{tableName}' exists: {count > 0}");

                    return count > 0;
                }
            }
        }
        catch (Exception ex)
        {
            Logger.Log($"Exception occurred during table existence check: {ex.Message}");
            return false;
        }
    }

        public void CreateTable(Logger logger, string connectionString, string tableName, string schemaXmlFilePath)
        {
            try
            {
                DataTable schemaTable = LoadSchemaTable(logger, tableName, schemaXmlFilePath);

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

                Logger.Log($"Table '{tableName}' created successfully.");
            }
            catch (Exception ex)
            {
                Logger.Log($"Exception occurred during table creation: {ex.Message}");
               //
               //Logger.Log($"Exception Stack Trace: {ex.StackTrace}");
            }

        }
        public static string GenerateCreateTableCommand(string tableName, DataTable schemaTable)
        {
            try
            {
                StringBuilder createTableCommand = new StringBuilder();
                createTableCommand.Append($"CREATE TABLE {tableName} (");

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
                columnDefinitions.Add("TimestampColumn DATETIME");

                Console.WriteLine($"Column Definitions: {string.Join(", ", columnDefinitions)}");

                createTableCommand.Append(string.Join(", ", columnDefinitions));

                // Append constraints only if there are any
                if (constraints.Count > 0)
                {
                    Console.WriteLine($"Constraints: {string.Join(", ", constraints)}");
                    createTableCommand.Append(", " + string.Join(", ", constraints));
                }

                createTableCommand.Append(");");

                Logger.Log($"SQL Command: {createTableCommand}");

                return createTableCommand.ToString();
            }
            catch (Exception ex)
            {
                Logger.Log($"Exception occurred during command generation: {ex.Message}");
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


        public DataTable LoadSchemaTable(Logger logger, string tableName, string schemaXmlFilePath)
        {
            DataTable schemaTable = new DataTable();

            try
            {
                XmlDocument schemaXmlDocument = new XmlDocument();
                schemaXmlDocument.Load(schemaXmlFilePath);

                //Console.WriteLine($"Schema XML: {schemaXmlDocument.OuterXml}");

                // Change the XPath to match the actual structure of your XML
                XmlNodeList enrollmentNodes = schemaXmlDocument.SelectNodes($"//DocumentElement/Students");

                // Console.WriteLine($"Number of Enrollment Nodes: {enrollmentNodes.Count}");

                foreach (XmlNode enrollmentNode in enrollmentNodes)
                {
                    //Console.WriteLine($"Enrollment Node XML: {enrollmentNode.OuterXml}");

                    // Extract column information
                    string columnName = enrollmentNode.SelectSingleNode("COLUMN_NAME")?.InnerText;
                    string dataType = enrollmentNode.SelectSingleNode("DATA_TYPE")?.InnerText;
                    string isNullable = enrollmentNode.SelectSingleNode("IS_NULLABLE")?.InnerText;
                    string isPrimaryKey = enrollmentNode.SelectSingleNode("IS_PRIMARY_KEY")?.InnerText;
                    string isForeignKey = enrollmentNode.SelectSingleNode("IS_FOREIGN_KEY")?.InnerText;

                    // Create DataColumn with correct data type and constraints
                    DataColumn column = new DataColumn(columnName, MapDataType(dataType));

                    if (isNullable?.ToLower() == "no")
                    {
                        column.AllowDBNull = false;
                    }

                    if (isPrimaryKey?.ToLower() == "true")
                    {
                        Console.WriteLine("Checked specifically for primary being true");
                        column.Unique = true;
                        column.ExtendedProperties["IS_PRIMARY_KEY"] = true;

                    }

                    if (isForeignKey?.ToLower() == "true")
                    {
                        column.ExtendedProperties["IS_FOREIGN_KEY"] = true;
                        column.ExtendedProperties["FOREIGN_KEY_TABLE"] = enrollmentNode.SelectSingleNode("./FOREIGN_KEY_TABLE")?.InnerText;
                        column.ExtendedProperties["FOREIGN_KEY_COLUMN"] = enrollmentNode.SelectSingleNode("./FOREIGN_KEY_COLUMN")?.InnerText;
                    }

                    // Add DataColumn to the DataTable
                    schemaTable.Columns.Add(column);
                    Console.WriteLine($"Number of Columns in schemaTable: {schemaTable.Columns.Count}");

                    // Debug statements to check constraints
                    Console.WriteLine($"Column Name: {column.ColumnName}, Data Type: {column.DataType}");
                    Console.WriteLine($"Allow DBNull: {column.AllowDBNull}");
                    Console.WriteLine($"Unique Constraint: {column.Unique}");
                    Console.WriteLine($"Foreign Key Constraint: {column.ExtendedProperties["IS_FOREIGN_KEY"]}");
                    Console.WriteLine($"Foreign Key Table: {column.ExtendedProperties["FOREIGN_KEY_TABLE"]}");
                    Console.WriteLine($"Foreign Key Column: {column.ExtendedProperties["FOREIGN_KEY_COLUMN"]}");
                }



                Logger.Log("Schema table loaded successfully.");
            }
            catch (Exception ex)
            {
                Logger.Log($"Exception occurred during schema table loading: {ex.Message}");
            }

            return schemaTable;
        }

        //public DataTable LoadSchemaTable(Logger logger, string tableName, string schemaXmlFilePath)
        //{
        //    DataTable schemaTable = new DataTable();

        //    try
        //    {
        //        XmlDocument schemaXmlDocument = new XmlDocument();
        //        schemaXmlDocument.Load(schemaXmlFilePath);

        //        XmlNodeList elements = schemaXmlDocument.SelectNodes($"//DocumentElement/Students");

        //        foreach (XmlNode elementNode in elements)
        //        {
        //            // Extract column information
        //            string columnName = elementNode.SelectSingleNode("COLUMN_NAME")?.InnerText;
        //            string dataType = elementNode.SelectSingleNode("DATA_TYPE")?.InnerText;
        //            string isNullable = elementNode.SelectSingleNode("IS_NULLABLE")?.InnerText;

        //            // Create DataColumn with correct data type and constraints
        //            DataColumn column = new DataColumn(columnName, MapDataType(dataType));

        //            if (isNullable?.ToLower() == "no")
        //            {
        //                column.AllowDBNull = false;
        //            }

        //            schemaTable.Columns.Add(column);
        //            Console.WriteLine($"Number of Columns in schemaTable: {schemaTable.Columns.Count}");

        //            // Debug statements to check constraints
        //            Console.WriteLine($"Column Name: {column.ColumnName}, Data Type: {column.DataType}");
        //            Console.WriteLine($"Allow DBNull: {column.AllowDBNull}");
        //        }

        //        Logger.Log("Schema table loaded successfully.");
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Log($"Exception occurred during schema table loading: {ex.Message}");
        //    }

        //    return schemaTable;
        //}



        public static Type MapDataType(string dataType)
    {
        switch (dataType.ToLower())
        {
            case "int":
                return typeof(int);
            case "varchar":
                return typeof(string);
            case "date":
                return typeof(DateTime);
            case "char":
                return typeof(char);
            default:
                return null;
        }
    }

  public   DataTable LoadDataTable(Logger logger, string tableName ,string dataXmlFilePath, DataTable schemaTable)
    {
        DataTable dataTable = new DataTable();

        try
        {
            XmlDocument dataXmlDocument = new XmlDocument();
            dataXmlDocument.Load(dataXmlFilePath);

            XmlNodeList elements = dataXmlDocument.SelectNodes($"//DocumentElement/Students");

            foreach (DataColumn column in schemaTable.Columns)
            {
                dataTable.Columns.Add(column.ColumnName, column.DataType);
            }
                dataTable.Columns.Add("TimestampColumn");
            foreach (XmlNode elementNode in elements)
            {
                DataRow newRow = dataTable.NewRow();

                foreach (DataColumn column in dataTable.Columns)
                {
                    string columnName = column.ColumnName;
                    string columnValue = GetInnerText(elementNode, columnName);

                    newRow[columnName] = Convert.ChangeType(columnValue, column.DataType);
                        newRow["TimestampColumn"] = Convert.ToString(DateTime.Now);
                }

                dataTable.Rows.Add(newRow);
            }

                Logger.Log("Data table loaded successfully.");
        }
        catch (Exception ex)
        {
            Logger.Log($"Exception occurred during data loading: {ex.Message}");
        }

        return dataTable;
    }

   public static string GetInnerText(XmlNode node, string childNodeName)
    {
        XmlNode childNode = node.SelectSingleNode(childNodeName);
        return childNode?.InnerText ?? string.Empty;
    }

        public void InsertDataInSql(Logger logger, DataTable dataTable, string connectionString, string tableName)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;

                        foreach (DataRow row in dataTable.Rows)
                        {
                            command.Parameters.Clear();

                            // Check if the record already exists
                            if (RecordExists(connection, tableName, row["StudentID"]))
                            {
                                // Update the existing record
                                UpdateRecord(command, dataTable, tableName, row);
                            }
                            else
                            {
                                // Insert a new record
                                InsertNewRecord(command, dataTable, tableName, row);
                            }
                        }
                    }
                }

                Logger.Log("Data inserted/updated into SQL Server table successfully.");
            }
            catch (Exception ex)
            {
                Logger.Log($"Exception occurred during data insertion: {ex.Message}");
            }
        }

        private bool RecordExists(SqlConnection connection, string tableName, object StudentID)
        {
            using (SqlCommand existsCommand = new SqlCommand($"SELECT 1 FROM {tableName} WHERE StudentID = @StudentID", connection))
            {
                existsCommand.Parameters.AddWithValue("@StudentID", StudentID);
                return existsCommand.ExecuteScalar() != null;
            }
        }
        private void UpdateRecord(SqlCommand command, DataTable dataTable, string tableName, DataRow row)
        {
            Logger.Log($"Updating record with StudentID ");

            // Construct the UPDATE statement based on your schema
            StringBuilder updateCommand = new StringBuilder($"UPDATE {tableName} SET ");

            foreach (DataColumn column in dataTable.Columns)
            {
                // Skip the primary key column in the update
                if (column.ColumnName != "StudentID")
                {
                    updateCommand.Append($"{column.ColumnName} = @{column.ColumnName}, ");

                    command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
                }
            }

            //command.Parameters.AddWithValue("@TimestampColumn", (DateTime.Now).ToString());

            // Remove the trailing comma and space
            updateCommand.Length -= 2;

            // Add WHERE clause to identify the record to update
            updateCommand.Append($" WHERE StudentID = @StudentID");

            command.CommandText = updateCommand.ToString();
            command.Parameters.AddWithValue("@StudentID", row["StudentID"]);

            // Execute the UPDATE command
            command.ExecuteNonQuery();
        }

        private void InsertNewRecord(SqlCommand command, DataTable dataTable, string tableName, DataRow row)
        {
            Logger.Log($"Inserting new record with StudentID ");


            // Construct the INSERT INTO statement based on your schema
            StringBuilder insertCommand = new StringBuilder($"INSERT INTO {tableName} (");
            StringBuilder valuesCommand = new StringBuilder("VALUES (");

            foreach (DataColumn column in dataTable.Columns)
            {
                insertCommand.Append($"{column.ColumnName}, ");
                valuesCommand.Append($"@{column.ColumnName}, ");
            }

            // Remove the trailing comma and space
            insertCommand.Length -= 2;
            valuesCommand.Length -= 2;

            insertCommand.Append(") ");
            valuesCommand.Append(")");

            // Combine INSERT INTO and VALUES statements
            insertCommand.Append(valuesCommand);

            command.CommandText = insertCommand.ToString();

            // Set parameters for each column
            foreach (DataColumn column in dataTable.Columns)
            {
                command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
            }
            //command.Parameters.AddWithValue("@TimestampColumn", (DateTime.Now).ToString()); 
            Console.WriteLine(command.CommandText);
            // Execute the INSERT command
            command.ExecuteNonQuery();
        }

}

}