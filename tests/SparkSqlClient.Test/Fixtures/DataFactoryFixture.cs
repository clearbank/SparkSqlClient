using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace SparkSqlClient.Test.Fixtures
{
    public class DataFactoryFixture
    {
        public DbCommand CreateCommand(DbConnection connection, string commandText)
        {
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            return command;
        }


        public async Task DropAndCreateTable(DbConnection connection, string tableName, IEnumerable<string> columns)
        {
            await CreateCommand(connection, $"DROP TABLE IF EXISTS {tableName}").ExecuteNonQueryAsync();
            await CreateCommand(connection, $"CREATE TABLE {tableName} ({string.Join(",", columns)}) USING DELTA").ExecuteNonQueryAsync();
        }

        public string TableName([System.Runtime.CompilerServices.CallerMemberName] string tableName = "")
        {
            return tableName;
        }
    }
}
