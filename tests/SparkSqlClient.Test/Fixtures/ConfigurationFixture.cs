using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace SparkSqlClient.Test.Fixtures
{
    public class AccessTokenCredentialsConfiguration
    {
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string Tenant { get; set; }
    }

    public class ConfigurationFixture
    {
        public IConfigurationRoot ConfigurationRoot { get; }

        public string ConnectionString => ConfigurationRoot.GetConnectionString("Spark");
        public AccessTokenCredentialsConfiguration AccessTokenCredentials => ConfigurationRoot.GetSection("AccessTokenCredentials").Get<AccessTokenCredentialsConfiguration>();

        public ConfigurationFixture()
        {
            ConfigurationRoot = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.debug.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }
    }
}
