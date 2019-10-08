using System.Collections.Generic;
using MySql.Data.MySqlClient;

namespace CsvToMySql
{
    public class DatabaseManager
    {
        public string ConnectionString { get; set; }

        public string TableName { get; set; }

        public List<string> ColumnName = new List<string>();

        public List<MySqlDbType> ColumnDataType = new List<MySqlDbType>();

        public List<int> ColumnDataSize = new List<int>();

        public bool IsValidConnectionString(string connectionString)
        {
            bool isValidConnectionString;
            try
            {
                using (MySqlConnection connection = new MySqlConnection(connectionString))
                {
                    connection.Open();
                    isValidConnectionString = true;
                }
            }
            catch
            {
                isValidConnectionString = false;
            }
            return isValidConnectionString;
        }

        public bool IsValidTableName(string tableName)
        {
            if (tableName == "")
            {
                return true;
            }
            return false;
        }

        public void CreateTable()
        {
            string sql = $"CREATE TABLE {TableName} (Id int NOT NULL AUTO_INCREMENT, PRIMARY KEY(Id))";

            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                connection.Open();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        public void AddColumn(string columnName, string dataType)
        {
            columnName = columnName.Replace(" ", "");
            string sql = $"ALTER TABLE {TableName} ADD {columnName} {dataType}";
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                connection.Open();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AddColumn(string columnName, string dataType, int dataSize)
        {
            columnName = columnName.Replace(" ", "");
            string sql = $"ALTER TABLE {TableName} ADD {columnName} {dataType}({dataSize})";
            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                connection.Open();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
        }

        public void AddRow(List<string> rowElements)
        {
            string columnName = GetColumnNameString(ColumnName);
            string verbatimValues = RowToVerbatim(ColumnName);


            string sql = $"INSERT INTO {TableName} ({columnName}) VALUES({verbatimValues})";

            using (MySqlConnection connection = new MySqlConnection(ConnectionString))
            {
                connection.Open();
                using (MySqlCommand command = new MySqlCommand())
                {
                    command.Connection = connection;
                    command.CommandText = sql;

                    int i = 0;
                    foreach (string element in ColumnName)
                    {
                        MySqlParameter mySqlParameter = new MySqlParameter();
                        mySqlParameter.ParameterName = '@' + element;
                        mySqlParameter.MySqlDbType = ColumnDataType[i];
                        mySqlParameter.Value = rowElements[i];
                        mySqlParameter.Size = ColumnDataSize[i];
                        command.Parameters.Add(mySqlParameter);
                        i++;
                    }
                    command.Connection = connection;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
        }

        private string GetColumnNameString(List<string> columnName)
        {
            string columnNameString = "";
            int i = 1;
            foreach(string name in columnName)
            {
                if (i != columnName.Count)
                {
                    columnNameString += name + ", ";
                }
                else
                {
                    columnNameString += name;
                }
                i++;
            }
            return columnNameString;
        }

        private string RowToVerbatim(List<string> rowElements)
        {
            string sqlValues = "";
            string separator = ", ";
            char verbatim = '@';
            int i = 0;
            int rowElementsNumber = rowElements.Count;
            foreach (string element in rowElements)
            {
                if (i == 0 || i == rowElementsNumber)
                {
                    sqlValues += verbatim + element;
                }
                else
                {
                    sqlValues += separator + verbatim + element;
                }
                i++;
            }
            return sqlValues;
        }

        public bool CheckDataType(string dataType)
        {
            string[] acceptedDataTypeCharacters =
            #region DataTypeCharacters
            {
                "binary",
                "bit",
                "blob",
                "byte",
                "date",
                "datetime",
                "decimal",
                "double",
                "enum",
                "float",
                "geometry",
                "guid",
                "smallint" ,
                "mediumint",
                "int",
                "bigint",
                "json",
                "longblob",
                "longtext",
                "mediumblob",
                "mediumtext",
                "newdate",
                "newdecimal",
                "set",
                "string",
                "text",
                "time",
                "timestamp",
                "tinyblob",
                "tinytext",
                "unsignedbyte",
                "unsignedsmallint",
                "unsignedmediumint",
                "unsignedint",
                "unsignedbigint",
                "varbinary",
                "varchar",
                "varstring",
                "year"
            };
            #endregion
            bool dataTypeCheck = false;

            for (int i = 0; i < acceptedDataTypeCharacters.Length; i++)
            {
                if (dataType == acceptedDataTypeCharacters[i])
                {
                    dataTypeCheck = true;
                    break;
                }
            }
            return dataTypeCheck;
        }

        public MySqlDbType StringToMySqlDbType(string type)
        {
            switch(type)
            {
                case "binary": return MySqlDbType.Binary;
                case "bit": return MySqlDbType.Bit;
                case "blob": return MySqlDbType.Blob;
                case "byte": return MySqlDbType.Byte;
                case "date": return MySqlDbType.Date;
                case "datetime": return MySqlDbType.DateTime;
                case "decimal": return MySqlDbType.Decimal;
                case "double": return MySqlDbType.Double;
                case "enum": return MySqlDbType.Enum;
                case "float": return MySqlDbType.Float;
                case "geometry": return MySqlDbType.Geometry;
                case "guid": return MySqlDbType.Guid;
                case "smallint": return MySqlDbType.Int16;
                case "mediumint": return MySqlDbType.Int24;
                case "int": return MySqlDbType.Int32;
                case "bigint": return MySqlDbType.Int64;
                case "json": return MySqlDbType.JSON;
                case "longblob": return MySqlDbType.LongBlob;
                case "longtext": return MySqlDbType.LongText;
                case "mediumblob": return MySqlDbType.MediumBlob;
                case "mediumtext": return MySqlDbType.MediumText;
                case "newdate": return MySqlDbType.Newdate;
                case "newdecimal": return MySqlDbType.NewDecimal;
                case "set": return MySqlDbType.Set;
                case "string": return MySqlDbType.String;
                case "text": return MySqlDbType.Text;
                case "time": return MySqlDbType.Time;
                case "timestamp": return MySqlDbType.Timestamp;
                case "tinyblob": return MySqlDbType.TinyBlob;
                case "tinytext": return MySqlDbType.TinyText;
                case "unsignedbyte": return MySqlDbType.UByte;
                case "unsignedsmallint": return MySqlDbType.UInt16;
                case "unsignedmediumint": return MySqlDbType.UInt24;
                case "unsignedint": return MySqlDbType.UInt32;
                case "unsignedbigint": return MySqlDbType.UInt64;
                case "varbinary": return MySqlDbType.VarBinary;
                case "varchar": return MySqlDbType.VarChar;
                case "varstring": return MySqlDbType.VarString;
                case "year": return MySqlDbType.Year;
                default: return 0;
            }
        }
    }
}
