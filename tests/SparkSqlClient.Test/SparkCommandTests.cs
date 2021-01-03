using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using SparkSqlClient;
using SparkSqlClient.Test.Fixtures;
using Xunit;

namespace SparkSqlClient.Test
{
    public class SparkCommandTests : IClassFixture<ConfigurationFixture>, IClassFixture<DataFactoryFixture>, IClassFixture<StartClusterFixture>
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
            var result = DataFactory.CreateCommand(conn, $@"INSERT INTO {table} VALUES 
                    (1), (2), (3)").ExecuteNonQuery();
            var resultAsync = await DataFactory.CreateCommand(conn, $@"INSERT INTO {table} VALUES 
                    (1), (2), (3)").ExecuteNonQueryAsync();

            // Assert
            Assert.Equal(-1, result);
            Assert.Equal(-1, resultAsync);
        }

        [Fact]
        public async Task WhenScalerShouldReturnFirstRow()
        {
            // Arrange
            var table = DataFactory.TableName();
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();
            await DataFactory.DropAndCreateTable(conn, table, new[]
            {
                "myInt INT"
            });
            await DataFactory.CreateCommand(conn, $@"INSERT INTO {table} VALUES 
                    (1), (2), (3)").ExecuteNonQueryAsync();

            // Act
            var command = DataFactory.CreateCommand(conn, $@"SELECT * FROM {table} ORDER BY myInt DESC");
            command.CommandTimeout = 1;
            var result = command.ExecuteScalar();
            var resultAsync = await command.ExecuteScalarAsync();

            // Assert
            Assert.Equal(3, result);
            Assert.Equal(3, resultAsync);
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



        [Fact]
        public async Task CommandTypeShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"SELECT 1");

            // Act and Assert
            command.CommandType = System.Data.CommandType.Text;
            Assert.Throws<NotSupportedException>(() => command.CommandType = System.Data.CommandType.StoredProcedure);
        }

        [Fact]
        public async Task DesignTimeVisibleShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"SELECT 1");

            // Act and Assert
            command.DesignTimeVisible = false;
            Assert.Throws<NotSupportedException>(() => command.DesignTimeVisible = true);
        }

        [Fact]
        public async Task UpdatedRowSourceShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"SELECT 1");

            // Act and Assert
            command.UpdatedRowSource = System.Data.UpdateRowSource.None;
            Assert.Throws<NotSupportedException>(() => command.UpdatedRowSource = System.Data.UpdateRowSource.FirstReturnedRecord);
        }

        [Fact]
        public async Task CancelShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"SELECT 1");

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => command.Cancel());
        }

        [Fact]
        public async Task PrepareShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            var command = DataFactory.CreateCommand(conn, $"SELECT 1");

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => command.Prepare());
        }
    }




}
