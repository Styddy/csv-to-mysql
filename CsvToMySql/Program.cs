using System;
using System.Collections.Generic;

namespace CsvToMySql
{
    class MainClass
    {
        public static void Main(string[] args)
        {
            List<string> columns;
            FileManager fileManager = new FileManager();
            DatabaseManager databaseManager = new DatabaseManager();

            Console.Title = "CsvToMySql";

            if (args[0].Length > 0)
            {
                fileManager.FilePath = args[0];
            }
            else
            {
                Console.Write("Paste the .csv path: ");
                fileManager.FilePath = Console.ReadLine();
            }

            Console.Write("Paste the database connection string: ");
            databaseManager.ConnectionString = Console.ReadLine();

            Console.Write("Enter the table name: ");
            databaseManager.TableName = Console.ReadLine();
            databaseManager.CreateTable();

            columns = fileManager.RowReader(0);

            foreach (string column in columns)
            {
                bool error;

                do
                {
                    Console.Write("Enter the data type of the column {0}: ", column);
                    string dataType = Console.ReadLine().ToLower();

                    if (databaseManager.CheckDataType(dataType))
                    {
                        Console.Write("Specify the data size of {0} (Enter 0 if {0} does not need a size): ", dataType);
                        int dataSize = int.Parse(Console.ReadLine());

                        if (dataSize != 0)
                        {
                            databaseManager.AddColumn(column, dataType, dataSize);
                        }
                        else
                        {
                            databaseManager.AddColumn(column, dataType);
                        }

                        databaseManager.ColumnName.Add(column.Replace(" ", ""));
                        databaseManager.ColumnDataType.Add(databaseManager.StringToMySqlDbType(dataType));
                        databaseManager.ColumnDataSize.Add(dataSize);

                        error = false;
                    }
                    else
                    {
                        Console.WriteLine("The data type \"{0}\" does not exist!", dataType);

                        error = true;
                    }
                } while (error);
            }

            int nRow = fileManager.RowCounter();

            for (int i = 1; i < nRow; i++)
            {
                Console.WriteLine("Loading row {0} of {1}", i, nRow);
                databaseManager.AddRow(fileManager.RowReader(i));
            }

            Console.WriteLine("File loaded successfully.");
            Console.ReadKey();
        }
    }
}
