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

        public void CreateTable()
        {
            string sql = $"CREATE TABLE {TableName} (Id int PRIMARY KEY IDENTITY(1,1))";

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
            string verbatimValues = RowToVerbatim(ColumnName);
            string sql = $"INSERT INTO {TableName} VALUES({verbatimValues})";

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
                        MySqlParameter sqlParameter = new MySqlParameter();
                        sqlParameter.ParameterName = '@' + element;
                        sqlParameter.MySqlDbType = ColumnDataType[i];
                        sqlParameter.Value = rowElements[i];
                        sqlParameter.Size = ColumnDataSize[i];
                        command.Parameters.Add(sqlParameter);
                        i++;
                    }
                    command.Connection = connection;
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
                connection.Close();
            }
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
            return type switch
            {
                "binary" => MySqlDbType.Binary,
                "bit" => MySqlDbType.Bit,
                "blob" => MySqlDbType.Blob,
                "byte" => MySqlDbType.Byte,
                "date" => MySqlDbType.Date,
                "datetime" => MySqlDbType.DateTime,
                "decimal" => MySqlDbType.Decimal,
                "double" => MySqlDbType.Double,
                "enum" => MySqlDbType.Enum,
                "float" => MySqlDbType.Float,
                "geometry" => MySqlDbType.Geometry,
                "guid" => MySqlDbType.Guid,
                "smallint" => MySqlDbType.Int16,
                "mediumint" => MySqlDbType.Int24,
                "int" => MySqlDbType.Int32,
                "bigint" => MySqlDbType.Int64,
                "json" => MySqlDbType.JSON,
                "longblob" => MySqlDbType.LongBlob,
                "longtext" => MySqlDbType.LongText,
                "mediumblob" => MySqlDbType.MediumBlob,
                "mediumtext" => MySqlDbType.MediumText,
                "newdate" => MySqlDbType.Newdate,
                "newdecimal" => MySqlDbType.NewDecimal,
                "set" => MySqlDbType.Set,
                "string" => MySqlDbType.String,
                "text" => MySqlDbType.Text,
                "time" => MySqlDbType.Time,
                "timestamp" => MySqlDbType.Timestamp,
                "tinyblob" => MySqlDbType.TinyBlob,
                "tinytext" => MySqlDbType.TinyText,
                "unsignedbyte" => MySqlDbType.UByte,
                "unsignedsmallint" => MySqlDbType.UInt16,
                "unsignedmediumint" => MySqlDbType.UInt24,
                "unsignedint" => MySqlDbType.UInt32,
                "unsignedbigint" => MySqlDbType.UInt64,
                "varbinary" => MySqlDbType.VarBinary,
                "varchar" => MySqlDbType.VarChar,
                "varstring" => MySqlDbType.VarString,
                "year" => MySqlDbType.Year,
                _ => 0,
            };
        }
    }
}
