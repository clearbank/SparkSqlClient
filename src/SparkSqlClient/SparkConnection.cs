using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.exceptions;
using SparkSqlClient.generated;
using SparkSqlClient.servicewrappers;
using Thrift.Protocol;
using Thrift.Transport.Client;

namespace SparkSqlClient
{
    /// <summary>
    /// Represents a connection to a spark server.
    /// </summary>
    public sealed class SparkConnection : DbConnection
    {
        internal readonly TCLIService.IAsync Client;

        private readonly TCLIService.Client _rawClient;

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

        public override ConnectionState State => SessionHandle != null ? ConnectionState.Open : ConnectionState.Closed;

        private string _accessToken;

        /// <summary>
        /// Gets or sets the access token. Access token is the Bearer token sent to the server for authentication.
        /// </summary>
        public string AccessToken {
            get => _accessToken;
            set
            {
                _accessToken = value;
                ConfigureClientAuth(_rawClient, ConnectionStringBuilder, AccessToken);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkConnection"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <exception cref="ArgumentException">Connection string must define a Data Source - connectionString</exception>
        public SparkConnection(string connectionString)
        {
            ConnectionStringBuilder = new SparkConnectionStringBuilder(connectionString);

            if (string.IsNullOrWhiteSpace(ConnectionStringBuilder.DataSource))
            {
                throw new ArgumentException("Connection string must define a Data Source", nameof(connectionString));
            }

            var httpTransport = new THttpTransport(new Uri(ConnectionStringBuilder.DataSource));
           _rawClient = new TCLIService.Client(new TBinaryProtocol(httpTransport));
            Client = new TCLIServiceSync(_rawClient);

            ConfigureClientAuth(_rawClient, ConnectionStringBuilder, AccessToken);
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
            if (SessionHandle != null)
            {
                
                await Client.CloseSessionAsync(new TCloseSessionReq
                {
                    SessionHandle = SessionHandle
                }, CancellationToken.None).ConfigureAwait(false);
                SessionHandle = null;
            }
        }

        public override void Open()
        {
            OpenAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            await _rawClient.OpenTransportAsync(cancellationToken).ConfigureAwait(false);
            var sessionResponse = await Client.OpenSessionAsync(new TOpenSessionReq(), cancellationToken).ConfigureAwait(false);

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

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                Close();
                _rawClient.Dispose();
            }
        }

        public override async ValueTask DisposeAsync()
        {
            await CloseAsync();
            _rawClient.Dispose();
        }

        private static void ConfigureClientAuth(TCLIService.Client client, SparkConnectionStringBuilder connectionString, string accessToken)
        {
            var transport = client.InputProtocol.Transport as THttpTransport
               ?? throw new Exception($"Unable to set authentication on {client.InputProtocol.Transport.GetType()}");

            if (accessToken != null)
            {
                transport.RequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", accessToken);
            }
            else if (connectionString.UserId != null && connectionString.Password != null)
            {
                var basicAuthValue = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes($"{connectionString.UserId}:{connectionString.Password}"));
                transport.RequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", basicAuthValue);
            }
            else
            {
                transport.RequestHeaders.Authorization = null;
            }
        }
    }
}