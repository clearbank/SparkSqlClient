using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.exceptions;
using SparkSqlClient.generated;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace SparkSqlClient
{
    /// <summary>
    /// Represents a connection to a spark server.
    /// </summary>
    public sealed class SparkConnection : DbConnection
    {
        internal readonly TCLIService.Client Client;

        internal readonly SparkConnectionStringBuilder ConnectionStringBuilder;

        internal TSessionHandle SessionHandle;


        public override string ConnectionString
        {
            get => ConnectionStringBuilder.ConnectionString;
            set => throw new NotSupportedException($"{nameof(SparkConnection)} does not support changing {nameof(ConnectionString)} after instantiation");
        }

        public override string Database => throw new NotSupportedException($"{nameof(SparkConnection)} does not support reading {nameof(Database)}");

        public override string DataSource => throw new NotSupportedException($"{nameof(SparkConnection)} does not support reading {nameof(DataSource)}");

        public override string ServerVersion => throw new NotSupportedException($"{nameof(SparkConnection)} does not support reading {nameof(ServerVersion)}");

        public override ConnectionState State => (Client.OutputProtocol?.Transport?.IsOpen ?? false) && SessionHandle != null ? ConnectionState.Open : ConnectionState.Closed;



        public SparkConnection(string connectionString)
        {
            ConnectionStringBuilder = new SparkConnectionStringBuilder(connectionString);

            var authHeader = "Basic " + Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes($"{ConnectionStringBuilder.UserId}:{ConnectionStringBuilder.Password}"));
            Client = new TCLIService.Client(new TBinaryProtocol(new THttpTransport(new Uri(ConnectionStringBuilder.DataSource), new Dictionary<string, string>
            {
                { "Authorization", authHeader }
            })));
        }


        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException($"{nameof(SparkConnection)} does not support changing databases");
        }

        public override void Close()
        {
            CloseAsync().GetAwaiter().GetResult();
        }

        public override async Task CloseAsync()
        {
            if (State == ConnectionState.Open && SessionHandle != null)
            {
                await Client.CloseSessionAsync(new TCloseSessionReq
                {
                    SessionHandle = SessionHandle
                }, CancellationToken.None).ConfigureAwait(false);
            }
        }

        public override void Open()
        {
            OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            await Client.OpenTransportAsync(cancellationToken).ConfigureAwait(false);
            var sessionResponse = await Client.OpenSessionAsync(new TOpenSessionReq()
            {
                Username = ConnectionStringBuilder.UserId,
                Password = ConnectionStringBuilder.Password
            }, cancellationToken).ConfigureAwait(false);

            SparkOperationException.ThrowIfInvalidStatus(sessionResponse.Status);

            SessionHandle = sessionResponse.SessionHandle;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException($"{nameof(SparkConnection)} does not support transactions");
        }

        protected override DbCommand CreateDbCommand()
        {
            return new SparkCommand(this);
        }

        public DbCommand CreateCommand(string commandText)
        {
            var command = CreateCommand();
            command.CommandText = commandText;
            return command;
        }
    }
}