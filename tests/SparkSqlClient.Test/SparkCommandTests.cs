using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using SparkSqlClient;
using SparkThrift.Test.Fixtures;
using Xunit;

namespace SparkThrift.Test
{
    public class SparkCommandTests : IClassFixture<ConfigurationFixture>, IClassFixture<DataFactoryFixture>
    {
        public ConfigurationFixture Config { get; }
        public DataFactoryFixture DataFactory { get; }

        public SparkCommandTests(ConfigurationFixture config, DataFactoryFixture dataFactory)
        {
            Config = config;
            DataFactory = dataFactory;
        }

        [Fact]
        public async Task WhenNonQueryShouldReturnUnknownRowsUpdated()
        {
            // Arrange
            var table = DataFactory.TableName();
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();
            await DataFactory.DropAndCreateTable(conn, table, new[]
            {
                "myInt INT"
            });

            // Act
            var result = await DataFactory.CreateCommand(conn, $@"INSERT INTO {table} VALUES 
                    (1), (2), (3)").ExecuteNonQueryAsync();

            // Assert
            Assert.Equal(-1, result);
        }


        [Fact]
        public async Task WhenAtParameterWithinQuotesShouldNotReplace()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            conn.Open();

            var cmd = DataFactory.CreateCommand(conn,$@"SELECT 
""This is \"" @MyValue \"" string"" as `@MyValue string`,
'This is also \' @MyValue \' string' as `also @MyValue string`,
@MyValue+3 as MyValue");

            var param = cmd.CreateParameter();
            param.ParameterName = "MyValue";
            param.Value = 12;
            cmd.Parameters.Add(param);

            // Act
            await using var resultReader = await cmd.ExecuteReaderAsync();

            // Assert
            Assert.True(await resultReader.ReadAsync());

            Assert.Equal("This is \" @MyValue \" string", resultReader["@MyValue string"]);
            Assert.Equal("This is also \' @MyValue \' string", resultReader["also @MyValue string"]);
            Assert.Equal(15, resultReader["MyValue"]);

            Assert.False(await resultReader.ReadAsync());
        }

        [Fact]
        public async Task WhenExecuteWithoutOpenShouldThrow()
        {
            // Arrange
            var table = DataFactory.TableName();
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"CREATE TABLE {table} (myInt INT) USING DELTA");
            
            // Act and Assert
            Assert.Throws<InvalidOperationException>(() => command.ExecuteReader());
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await command.ExecuteReaderAsync());
            Assert.Throws<InvalidOperationException>(() => command.ExecuteScalar());
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await command.ExecuteScalarAsync());
            Assert.Throws<InvalidOperationException>(() => command.ExecuteNonQuery());
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await command.ExecuteNonQueryAsync());
        }
    }




}
