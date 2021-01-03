using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using SparkSqlClient;
using SparkSqlClient.exceptions;
using SparkSqlClient.Test.Fixtures;
using Xunit;

namespace SparkSqlClient.Test
{
    public class TypeRainbow
    {
        public static string[] TableColumns = new[]
        {
            "myBigInt BIGINT NOT NULL",
            "myInt INT NOT NULL",
            "mySmallInt SMALLINT NOT NULL",
            "myTinyInt TINYINT NOT NULL",
            "myBoolean BOOLEAN NOT NULL",
            "myDouble Double NOT NULL",
            "myFloat FLOAT NOT NULL",
            "myDecimal DECIMAL(10, 2) NOT NULL",
            "myString STRING NOT NULL",
            "myDate DATE NOT NULL",
            "myTimestamp TIMESTAMP NOT NULL",
            "myBinary BINARY NOT NULL",
            "myArray ARRAY<STRING> NOT NULL",
            "myMap MAP<STRING, INT> NOT NULL",
        };

        public long MyBigInt { get; set; }
        public int MyInt { get; set; }
        public Int16 MySmallInt { get; set; }
        public byte MyTinyInt { get; set; }
        public bool MyBoolean { get; set; }
        public double MyDouble { get; set; }
        public float MyFloat { get; set; }
        public decimal MyDecimal { get; set; }
        public string MyString { get; set; }
        public DateTime MyDate { get; set; }
        public DateTime MyTimestamp { get; set; }
        public byte[] MyBinary { get; set; }
        public string MyArray { get; set; }
        public string MyMap { get; set; }
    }

    public class DapperTests : IClassFixture<ConfigurationFixture>, IClassFixture<DataFactoryFixture>, IClassFixture<StartClusterFixture>
    {
        public ConfigurationFixture Config { get; }
        public DataFactoryFixture DataFactory { get; }

        public DapperTests(ConfigurationFixture config, DataFactoryFixture dataFactory)
        {
            Config = config;
            DataFactory = dataFactory;
        }

        [Fact]
        public async Task WhenDapperShouldExecute()
        {
            var timestamp = new DateTime(2055, 3, 1, 21, 33, 43, 432);
            var date = new DateTime(2055, 3, 1, 21, 33, 43, 432).Date;

            var table = DataFactory.TableName();
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();
            await DataFactory.DropAndCreateTable(conn, table, TypeRainbow.TableColumns);
            await conn.ExecuteAsync($@"INSERT INTO {table} VALUES 
                    (
                        @MyBigInt, 
                        @MyInt, 
                        @MySmallInt, 
                        @MyTinyInt, 
                        @MyBoolean,
                        @MyDouble, 
                        @MyFloat, 
                        @MyDecimal,
                        @MyString,
                        @MyDate,
                        @MyTimestamp,
                        @MyBinary,
                        array('AAA', 'BBB', 'CCC'),
                        map('AAA', 1, 'BBB', 2, 'CCC', 3)
                    )", new TypeRainbow()
            {
                MyBigInt = Int64.MaxValue,
                MyInt = Int32.MaxValue,
                MySmallInt = Int16.MaxValue,
                MyTinyInt = Byte.MaxValue,
                MyBoolean = true,
                MyDouble = 99999999.99d,
                MyFloat = 99999999.99f,
                MyDecimal = 99999999.99m,
                MyString = "AAA",
                MyDate = date,
                MyTimestamp = timestamp,
                MyBinary = new byte[] {0x48, 0x65, 0x6c, 0x6c, 0x6f}
            });

            var results = await conn.QueryAsync<TypeRainbow>($"SELECT * FROM {table}");

            var result = Assert.Single(results);

            Assert.Equal(Int64.MaxValue, result.MyBigInt);
            Assert.Equal(Int32.MaxValue, result.MyInt);
            Assert.Equal(Int16.MaxValue, result.MySmallInt);
            Assert.Equal(Byte.MaxValue, result.MyTinyInt);
            Assert.True(result.MyBoolean);
            Assert.Equal(99999999.99d, result.MyDouble);
            Assert.Equal(99999999.99f, result.MyFloat);
            Assert.Equal(99999999.99m, result.MyDecimal);
            Assert.Equal("AAA", result.MyString);
            Assert.Equal(date, result.MyDate);
            Assert.Equal(timestamp, result.MyTimestamp);
            Assert.Equal(new byte[] {0x48, 0x65, 0x6c, 0x6c, 0x6f}, result.MyBinary);
            Assert.Equal(@"[""AAA"",""BBB"",""CCC""]", result.MyArray);
            Assert.Equal(@"{""AAA"":1,""BBB"":2,""CCC"":3}", result.MyMap);
        }

        [Fact]
        public async Task WhenNullFirstOfManyItemsShouldReturn()
        {
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();

            var tableName = DataFactory.TableName();
            int order = 0;
            await DataFactory.DropAndCreateTable(conn, tableName, new[] { "order INT", "value INT" });
           
            await conn.ExecuteAsync($"INSERT INTO {tableName} VALUES ({order++}, null)");

            Task.WaitAll(Enumerable.Range(1, 16).Select(i => 
                conn.ExecuteAsync($"INSERT INTO {tableName} VALUES ({Interlocked.Increment(ref order)}, {i})")
            ).ToArray());

            var result = await conn.QueryAsync<int?>($"SELECT value FROM {tableName} ORDER BY order");

            var resultList = result.ToList();
            Assert.Equal(17, resultList.Count);
            Assert.Equal(new int?[]{null}.Concat(Enumerable.Range(1,16).Cast<int?>()).ToList(), resultList);
            
        }

        [Fact]
        public async Task WhenParallelisationShouldSucceed()
        {
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();

            var tableName = DataFactory.TableName();
           
            int order = 0;
            await DataFactory.DropAndCreateTable(conn, tableName, new[] { "order INT", "value INT" });

            await Task.WhenAll(Enumerable.Range(1, 5).Select(async _ =>
            {
                await using var insertConn = new SparkConnection(Config.ConnectionString);
                await insertConn.OpenAsync().ConfigureAwait(false);
                await Task.WhenAll(Enumerable.Range(1, 5).Select(async i =>
                {
                    var values = string.Join(",", Enumerable.Range(1, 40000).Select(i => $"({ Interlocked.Increment(ref order)}, {i})"));
                    await insertConn.ExecuteAsync($"INSERT INTO {tableName} VALUES {values}").ConfigureAwait(false);
                }));
            }).ToArray());


            var result = await conn.QueryAsync<int?>($"SELECT value FROM {tableName} ORDER BY random()");
            var resultList = result.ToList();
            Assert.Equal(1000000, resultList.Count);
        }

        [Fact]
        public async Task WhenParameterMissingShouldThrow()
        {
            var table = DataFactory.TableName();
            await using var conn = new SparkConnection(Config.ConnectionString);
            await conn.OpenAsync();
            await DataFactory.DropAndCreateTable(conn, table, new[] { "myString STRING NOT NULL" });

            var ex = await Assert.ThrowsAsync<MissingParameterException>(() => conn.ExecuteAsync($@"INSERT INTO {table} VALUES (@DoesNotExist)", new
            {
                DoesExist = "test"
            }));

            Assert.Equal("DoesNotExist", ex.ParameterName);
        }



    }
}
