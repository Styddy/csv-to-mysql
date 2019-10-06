using System.IO;

namespace CsvToMySql
{
    public static class HelperConnection
    {
        public static string GetConnectionString()
        {
            string defaultString = "connectionstring.txt";
            return GetConnectionString(defaultString);
        }

        public static string GetConnectionString(string path)
        {
            string connectionString;

            using (FileStream stream = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                using (StreamReader reader = new StreamReader(stream))
                {
                    connectionString = reader.ReadToEnd();

                    reader.Close();
                }
                stream.Close();
            }
            return connectionString;
        }
    }
}
