using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SparkSqlClient;
using SparkSqlClient.Test.Fixtures;
using Xunit;

namespace SparkSqlClient.Test
{
    public class SparkConnectionTests : IClassFixture<ConfigurationFixture>, IClassFixture<DataFactoryFixture>, IClassFixture<StartClusterFixture>
    {
        public ConfigurationFixture Config { get; }
        public DataFactoryFixture DataFactory { get; }

        public SparkConnectionTests(ConfigurationFixture config, DataFactoryFixture dataFactory)
        {
            Config = config;
            DataFactory = dataFactory;
        }

        [Fact(Skip = "test")]
        public async Task OpenCloseShouldChangeState()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);

            // Assert and Act  
            conn.Close();
            Assert.Equal(ConnectionState.Closed, conn.State);

            // Assert and Act  
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);
        }

        [Fact(Skip ="test")]
        public async Task ShouldAcceptAccessToken()
        {
            var httpClient = new HttpClient();
            var response = await httpClient.PostAsync($"https://login.microsoftonline.com/{Config.AccessTokenCredentials.Tenant}/oauth2/v2.0/token", new FormUrlEncodedContent(new Dictionary<string, string>()
            {
                { "grant_type", "client_credentials" },
                { "scope", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"},
                { "client_id", Config.AccessTokenCredentials.ClientId},
                { "client_secret", Config.AccessTokenCredentials.ClientSecret},
                
            }));
            response.EnsureSuccessStatusCode();
            var accessToken = JsonConvert.DeserializeObject<JObject>(await response.Content.ReadAsStringAsync())["access_token"].ToString();
            

            // Arrange
            var connectionStringBuilder = new SparkConnectionStringBuilder(Config.ConnectionString);
            await using var conn = new SparkConnection($"Data Source={connectionStringBuilder.DataSource}");
            conn.AccessToken = accessToken;
           
            // Assert and Act  
            conn.Open();
            Assert.Equal(ConnectionState.Open, conn.State);
        }

        [Fact]
        public void ConnectionStringShouldContainDatasource()
        {
            // Act and Assert
            Assert.Throws<ArgumentException>(() => new SparkConnection("Username=token"));
        }

        [Fact]
        public async Task ConnectionStringShouldBeReadonly()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);

            // Act and Assert
            
            Assert.Equal(new SparkConnectionStringBuilder(Config.ConnectionString).ConnectionString, conn.ConnectionString, ignoreCase:true);
            Assert.Throws<NotSupportedException>(() => conn.ConnectionString = Config.ConnectionString);
        }

        [Fact]
        public async Task BeginTransactionShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);
            
            // Act and Assert
            Assert.Throws<NotSupportedException>(() => conn.BeginTransaction());
            await Assert.ThrowsAsync<NotSupportedException>(async () => await conn.BeginTransactionAsync());
        }

        [Fact]
        public async Task ChangeDatabaseShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => conn.ChangeDatabase("db2"));
            await Assert.ThrowsAsync<NotSupportedException>(() => conn.ChangeDatabaseAsync("db2"));
        }

        [Fact]
        public async Task DatabaseShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => conn.Database);
        }

        [Fact]
        public async Task DataSourceShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => conn.DataSource);
        }

        [Fact]
        public async Task ServerVersionShouldNotBeSupported()
        {
            // Arrange
            await using var conn = new SparkConnection(Config.ConnectionString);

            // Act and Assert
            Assert.Throws<NotSupportedException>(() => conn.ServerVersion);
        }
    }




}
