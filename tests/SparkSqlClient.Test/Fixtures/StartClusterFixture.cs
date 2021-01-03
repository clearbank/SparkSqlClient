using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Transport;
using Xunit;

namespace SparkSqlClient.Test.Fixtures
{
    class StartClusterFixture : ConfigurationFixture, IAsyncLifetime
    {
        public StartClusterFixture()
        {

        }

        public async Task InitializeAsync()
        {
            var timeoutTokenSource = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
            timeoutTokenSource.CancelAfter(TimeSpan.FromMinutes(10));
            var cancellationToken = timeoutTokenSource.Token;


            while (true)
            {
                using (var connection = new SparkConnection(ConnectionString))
                {
                    try
                    {
                        // cluster will start, but fail until it finishing initilizaing
                        // so we are going to keep trying until it works
                        connection.Open();
                        return;
                    }
                    catch (TTransportException ex) when (ex.Message.Contains("503 (Service Unavailable)"))
                    {
                        await Task.Delay(5000, cancellationToken);
                    }
                }
            }
            
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

    }
}
