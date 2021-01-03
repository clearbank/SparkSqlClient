using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient;
using SparkSqlClient.exceptions;
using SparkSqlClient.Test.Fixtures;
using Thrift.Protocol;
using Xunit;

namespace SparkSqlClient.Test
{
    public class SparkDataReaderTests : IClassFixture<ConfigurationFixture>, IClassFixture<DataFactoryFixture>, IClassFixture<StartClusterFixture>, IDisposable
    {
        public ConfigurationFixture Config { get; }
        public DataFactoryFixture DataFactory { get; }
        public DbConnection Connection { get; }

        public SparkDataReaderTests(ConfigurationFixture config, DataFactoryFixture dataFactory)
        {
            Config = config;
            DataFactory = dataFactory;

            Connection = new SparkConnection(Config.ConnectionString);
            Connection.Open();
        }

        public void Dispose()
        {
            Connection.Dispose();
        }

        [Fact]
        public async Task WhenGetValuesShouldReturnAllColumns()
        {
            var tableName = nameof(WhenGetValuesShouldReturnAllColumns);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] {"myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (2, 3.4, 5.6)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();
            
            Assert.True(await reader.ReadAsync());

            Assert.Equal(3, reader.FieldCount);
            Assert.Equal(3, reader.VisibleFieldCount);

            var output = new object[reader.FieldCount];
            Assert.Equal(3, reader.GetValues(output));
            Assert.Equal(2, output[0]);
            Assert.Equal(3.4m, output[1]);
            Assert.Equal(5.6f, output[2]);
        }

        [Fact]
        public async Task ShouldConvertNameToOrdinal()
        {
            var tableName = nameof(ShouldConvertNameToOrdinal);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (2, 3.4, 5.6)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.Equal("myValue1", reader.GetName(0));
            Assert.Equal("myValue2", reader.GetName(1));
            Assert.Equal("myValue3", reader.GetName(2));

            Assert.Equal(0, reader.GetOrdinal("myValue1"));
            Assert.Equal(1, reader.GetOrdinal("myValue2"));
            Assert.Equal(2, reader.GetOrdinal("myValue3"));

            var invalidColumnNameException =  Assert.Throws<InvalidColumnNameException>(()=>reader.GetOrdinal("myValue4"));
            Assert.Equal("myValue4", invalidColumnNameException.ColumnName);
            Assert.Equal(new[]{ "myValue1" , "myValue2" , "myValue3" }, invalidColumnNameException.AvailableColumnNames);
        }

        [Fact]
        public async Task ShouldGetTypeMetadata()
        {
            var tableName = nameof(ShouldGetTypeMetadata);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[]
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
                "myArray ARRAY<STRING> NOT NULL",
                "myMap MAP<STRING, INT> NOT NULL",
                "myStruct STRUCT<`a`:INT, `b`:STRING>",
            });

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.Equal("bigint", reader.GetDataTypeName(0));
            Assert.Equal("int", reader.GetDataTypeName(1));
            Assert.Equal("smallint", reader.GetDataTypeName(2));
            Assert.Equal("tinyint", reader.GetDataTypeName(3));
            Assert.Equal("boolean", reader.GetDataTypeName(4));
            Assert.Equal("double", reader.GetDataTypeName(5));
            Assert.Equal("float", reader.GetDataTypeName(6));
            Assert.Equal("decimal", reader.GetDataTypeName(7));
            Assert.Equal("string", reader.GetDataTypeName(8));
            Assert.Equal("date", reader.GetDataTypeName(9));
            Assert.Equal("timestamp", reader.GetDataTypeName(10));
            Assert.Equal("array", reader.GetDataTypeName(11));
            Assert.Equal("map", reader.GetDataTypeName(12));
            Assert.Equal("struct", reader.GetDataTypeName(13));

            Assert.Equal(typeof(long), reader.GetFieldType(0));
            Assert.Equal(typeof(int), reader.GetFieldType(1));
            Assert.Equal(typeof(Int16), reader.GetFieldType(2));
            Assert.Equal(typeof(byte), reader.GetFieldType(3));
            Assert.Equal(typeof(bool), reader.GetFieldType(4));
            Assert.Equal(typeof(double), reader.GetFieldType(5));
            Assert.Equal(typeof(float), reader.GetFieldType(6));
            Assert.Equal(typeof(decimal), reader.GetFieldType(7));
            Assert.Equal(typeof(string), reader.GetFieldType(8));
            Assert.Equal(typeof(DateTime), reader.GetFieldType(9));
            Assert.Equal(typeof(DateTime), reader.GetFieldType(10));
            Assert.Equal(typeof(string), reader.GetFieldType(11));
            Assert.Equal(typeof(string), reader.GetFieldType(12));
            Assert.Equal(typeof(string), reader.GetFieldType(13));
        }

        [Fact]
        public async Task WhenSchemaTableThenNotSupported()
        {
            var tableName = nameof(WhenSchemaTableThenNotSupported);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (2, 3.4, 5.6)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.Throws<NotSupportedException>(() => reader.GetSchemaTable());
        }

        [Fact]
        public async Task WhenEmptyReadShouldBeFalse()
        {
            var tableName = nameof(WhenEmptyReadShouldBeFalse);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });
            
            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.False(await reader.ReadAsync());
            Assert.False(reader.HasRows);
        }

        [Fact]
        public async Task WhenDepthShouldBeZero()
        {
            var tableName = nameof(WhenDepthShouldBeZero);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.Equal(0, reader.Depth);
        }

        [Fact]
        public async Task WhenRecordsAffectedShouldBeNegativeOne()
        {
            var tableName = nameof(WhenDepthShouldBeZero);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.Equal(-1, reader.RecordsAffected);
        }

        [Fact]
        public async Task WhenClosedShouldFail()
        {
            var tableName = nameof(WhenDepthShouldBeZero);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT", "myValue2 DECIMAL(5,2)", "myValue3 FLOAT" });

            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (2, 3.4, 5.6)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();
            Assert.False(reader.IsClosed);
            reader.Dispose();

            Assert.True(reader.IsClosed);
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.ReadAsync());
        }


        [Fact]
        public async Task WhenMultipleRowsShouldReadAll()
        {
            var tableName = nameof(WhenDepthShouldBeZero);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue1 INT" });

            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (1),(2),(3)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();
            
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetInt32(0));

            Assert.True(reader.Read());
            Assert.Equal(2, reader.GetInt32(0));

            Assert.True(await reader.ReadAsync());
            Assert.Equal(3, reader.GetInt32(0));

            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task WhenInvalidColumnShouldError()
        {
            var tableName = nameof(WhenNullShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue INT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (42)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            void AssertInvalidOrdinal(Func<object> action)
            {
                var ex = Assert.Throws<InvalidOrdinalException>(action);
                Assert.Equal(1, ex.Ordinal);
                Assert.Equal(1, ex.ColumnCount);
            }

            AssertInvalidOrdinal(() => reader.IsDBNull(1));

            AssertInvalidOrdinal(() => reader.GetValue(1));

            var ex = Assert.Throws<InvalidColumnNameException>(() => reader["notMyValue"]);
            Assert.Equal("notMyValue", ex.ColumnName);
            Assert.Equal(new[] { "myValue" }, ex.AvailableColumnNames);

            AssertInvalidOrdinal(() => reader.GetInt64(1));
            AssertInvalidOrdinal(() => reader.GetInt32(1));
            AssertInvalidOrdinal(() => reader.GetInt16(1));
            AssertInvalidOrdinal(() => reader.GetByte(1));
            AssertInvalidOrdinal(() => reader.GetDecimal(1));
            AssertInvalidOrdinal(() => reader.GetDouble(1));
            AssertInvalidOrdinal(() => reader.GetFloat(1));
            AssertInvalidOrdinal(() => reader.GetChar(1));
            AssertInvalidOrdinal(() => reader.GetString(1));
            AssertInvalidOrdinal(() => reader.GetBoolean(1));
            AssertInvalidOrdinal(() => reader.GetDateTime(1));
            AssertInvalidOrdinal(() => reader.GetGuid(1));
        }


        [Fact]
        public async Task WhenNullShouldRead()
        {
            var tableName = nameof(WhenNullShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue INT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (NULL)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.True(reader.IsDBNull(0));
            Assert.True(await reader.IsDBNullAsync(0));

            Assert.IsType<DBNull>(reader.GetValue(0));
            Assert.Equal(DBNull.Value, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(int), "int");
            
            var nullEx = Assert.Throws<NullValueException>(() => reader.GetInt32(0));
            Assert.Equal(0, nullEx.Ordinal);
            Assert.Equal(typeof(int), nullEx.RequestedType);

            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(int), "int");
        }

        [Fact]
        public async Task WhenLongShouldRead()
        {
            var tableName = nameof(WhenLongShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] {"myValue BIGINT"});
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (42)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();
            
            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(42L, reader.GetValue(0));
            Assert.Equal(42L, reader["myValue"]);

            Assert.Equal(42, reader.GetInt64(0));
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(long), "bigint");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(long), "bigint");
        }

        [Fact]
        public async Task WhenIntShouldRead()
        {
            var tableName = nameof(WhenIntShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue INT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (42)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(42, reader.GetValue(0));
            Assert.Equal(42, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(int), "int");
            Assert.Equal(42, reader.GetInt32(0));
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(int), "int");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(int), "int");
        }

        [Fact]
        public async Task WhenSmallIntShouldRead()
        {
            var tableName = nameof(WhenSmallIntShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue SMALLINT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (42)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal((Int16)42, reader.GetValue(0));
            Assert.Equal((Int16)42, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(short), "smallint");
            Assert.Equal(42, reader.GetInt16(0));
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(short), "smallint");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(short), "smallint");
        }

        [Fact]
        public async Task WhenTinyIntShouldRead()
        {
            var tableName = nameof(WhenTinyIntShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue TINYINT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (42)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal((byte)42, reader.GetValue(0));
            Assert.Equal((byte)42, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(Byte), "tinyint");
            Assert.Equal(42, reader.GetByte(0));
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(Byte), "tinyint");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(Byte), "tinyint");
        }

        [Fact]
        public async Task WhenDecimalShouldRead()
        {
            var tableName = nameof(WhenDecimalShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue DECIMAL(10,5)" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (4.2)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(4.2m, reader.GetValue(0));
            Assert.Equal(4.2m, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(decimal), "decimal");
            Assert.Equal(4.2m, reader.GetDecimal(0));
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(decimal), "decimal");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(decimal), "decimal");
        }

        [Fact]
        public async Task WhenDoubleShouldRead()
        {
            var tableName = nameof(WhenDoubleShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue DOUBLE" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (4.2)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(4.2d, reader.GetValue(0));
            Assert.Equal(4.2d, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(Double), "double");
            Assert.Equal(4.2d, reader.GetDouble(0));
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(Double), "double");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(Double), "double");
        }

        [Fact]
        public async Task WhenFloatShouldRead()
        {
            var tableName = nameof(WhenFloatShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue FLOAT" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (4.2)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(4.2f, reader.GetValue(0));
            Assert.Equal(4.2f, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(Single), "float");
            Assert.Equal(4.2f, reader.GetFloat(0));
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(Single), "float");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(Single), "float");
        }


        [Fact]
        public async Task WhenStringShouldRead()
        {
            var tableName = nameof(WhenStringShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue VARCHAR(2)" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES ('42')").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal("42", reader.GetValue(0));
            Assert.Equal("42", reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(String), "string");
            Assert.Equal("42", reader.GetString(0));
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(String), "string");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(String), "string");
        }

        [Fact]
        public async Task WhenBooleanShouldRead()
        {
            var tableName = nameof(WhenBooleanShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue BOOLEAN" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (true)").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(true, reader.GetValue(0));
            Assert.Equal(true, reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(Boolean), "boolean");
            Assert.True(reader.GetBoolean(0));
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(Boolean), "boolean");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(Boolean), "boolean");
        }

        [Fact]
        public async Task WhenDateShouldRead()
        {
            var tableName = nameof(WhenDateShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue Date" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES ('4242-04-24')").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(new DateTime(4242, 04, 24), reader.GetValue(0));
            Assert.Equal(new DateTime(4242, 04, 24), reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(DateTime), "date");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(DateTime), "date");
            Assert.Equal(new DateTime(4242,04, 24), reader.GetDateTime(0));
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(DateTime), "date");
        }

        [Fact]
        public async Task WhenTimestampShouldRead()
        {
            var tableName = nameof(WhenTimestampShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue Timestamp" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES ('4242-04-24 04:42:42')").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(new DateTime(4242, 04, 24, 4, 42, 42), reader.GetValue(0));
            Assert.Equal(new DateTime(4242, 04, 24, 4, 42, 42), reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetString(0), 0, typeof(DateTime), "timestamp");
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(DateTime), "timestamp");
            Assert.Equal(new DateTime(4242, 04, 24, 4, 42, 42), reader.GetDateTime(0));
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(DateTime), "timestamp");
        }

        [Fact]
        public async Task WhenArrayShouldRead()
        {
            var tableName = nameof(WhenArrayShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue ARRAY<STRING>" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (array('a','b','c'))").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(@"[""a"",""b"",""c""]", reader.GetValue(0));
            Assert.Equal(@"[""a"",""b"",""c""]", reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(string), "array");
            Assert.Equal(@"[""a"",""b"",""c""]", reader.GetString(0));
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(string), "array");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(string), "array");
        }

        [Fact]
        public async Task WhenMapShouldRead()
        {
            var tableName = nameof(WhenMapShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue MAP<STRING, INT>" });
            await DataFactory.CreateCommand(Connection, $"INSERT INTO {tableName} VALUES (map('a',1,'b',2,'c',3))").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(@"{""a"":1,""b"":2,""c"":3}", reader.GetValue(0));
            Assert.Equal(@"{""a"":1,""b"":2,""c"":3}", reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(string), "map");
            Assert.Equal(@"{""a"":1,""b"":2,""c"":3}", reader.GetString(0));
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(string), "map");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(string), "map");
        }

        [Fact]
        public async Task WhenStructShouldRead()
        {
            var tableName = nameof(WhenStructShouldRead);
            await DataFactory.DropAndCreateTable(Connection, tableName, new[] { "myValue STRUCT<`a`:STRING, `b`:INT>" });
            await DataFactory.CreateCommand(Connection, $@"INSERT INTO {tableName} VALUES (from_json('{{""a"":""hello"",""b"":2}}', 'a STRING, b INT'))").ExecuteNonQueryAsync();

            var reader = await DataFactory.CreateCommand(Connection, $"SELECT * FROM {tableName}").ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());

            Assert.False(reader.IsDBNull(0));
            Assert.False(await reader.IsDBNullAsync(0));

            Assert.Equal(@"{""a"":""hello"",""b"":2}", reader.GetValue(0));
            Assert.Equal(@"{""a"":""hello"",""b"":2}", reader["myValue"]);

            AssertInvalidDataTypeException(() => reader.GetInt64(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetInt32(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetInt16(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetByte(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetDecimal(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetDouble(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetFloat(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetChar(0), 0, typeof(string), "struct");
            Assert.Equal(@"{""a"":""hello"",""b"":2}", reader.GetString(0));
            AssertInvalidDataTypeException(() => reader.GetBoolean(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetDateTime(0), 0, typeof(string), "struct");
            AssertInvalidDataTypeException(() => reader.GetGuid(0), 0, typeof(string), "struct");
        }


        protected void AssertInvalidDataTypeException<T>(Func<T> action, int ordinal, Type actualType, string sparkType)
        {
            var ex = Assert.Throws<InvalidDataTypeException>(() => action());
            Assert.Equal(ordinal, ex.Ordinal);
            Assert.Equal(typeof(T), ex.RequestedType);
            Assert.Equal(actualType, ex.ActualType);
            Assert.Equal(sparkType, ex.ActualTypeName);
        }
    }
}
