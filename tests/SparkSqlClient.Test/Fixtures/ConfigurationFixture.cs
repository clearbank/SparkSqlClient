using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace SparkSqlClient.Test.Fixtures
{
    public class ConfigurationFixture
    {
        public IConfigurationRoot ConfigurationRoot { get; }

        public string ConnectionString => ConfigurationRoot.GetConnectionString("Spark");

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
