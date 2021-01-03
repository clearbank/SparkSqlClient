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
    public class SparkConnectionStringBuilderTests
    {

        [Theory]
        [InlineData("Data Source=https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000; User Id=token; Password=dapi00000000", 
            "https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000", "token", "dapi00000000")]

        [InlineData("DATA SOURCE=https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000; USER ID=token; PASSWORD=dapi00000000",
            "https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000", "token", "dapi00000000")]

        [InlineData("Data Source=https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000; User Id=token; Pwd=dapi00000000",
            "https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000", "token", "dapi00000000")]

        [InlineData("Data Source=https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000; User Id=token; Pwd=dapi00000000; irrelavent=true",
            "https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000", "token", "dapi00000000")]
        public void WhenParsingConnectionStringShouldParseImportantValues(string connectionString, string expectedDataSource, string expectedUserId, string expectedPassword)
        {
            // Act
            var sparkConnectionString = new SparkConnectionStringBuilder(connectionString);

            // Assert
            Assert.Equal(expectedDataSource, sparkConnectionString.DataSource);
            Assert.Equal(expectedUserId, sparkConnectionString.UserId);
            Assert.Equal(expectedPassword, sparkConnectionString.Password);
        }

        [Fact]
        public void WhenSettingConnectionStringValuesShouldGetSameValues()
        {
            // Act
            var sparkConnectionString = new SparkConnectionStringBuilder();
            sparkConnectionString.DataSource = "https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000";
            sparkConnectionString.UserId = "token";
            sparkConnectionString.Password = "dapi00000000";

            // Assert
            Assert.Equal("https://adb-0000000000000000.00.azuredatabricks.net/sql/protocolv1/o/0000000000000000/0000-000000-xxxxx000", sparkConnectionString.DataSource);
            Assert.Equal("token", sparkConnectionString.UserId);
            Assert.Equal("dapi00000000", sparkConnectionString.Password);
        }
    }




}
