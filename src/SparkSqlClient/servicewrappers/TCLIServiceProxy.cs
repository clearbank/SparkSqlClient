using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.generated;

namespace SparkSqlClient.servicewrappers
{
    internal abstract class TCLIServiceProxy : TCLIService.IAsync
    {
        public abstract Task<TResult> Proxy<TResult>(string method, Func<TCLIService.IAsync, CancellationToken, Task<TResult>> action, CancellationToken cancellationToken);
        

        public Task<TOpenSessionResp> OpenSessionAsync(TOpenSessionReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(OpenSessionAsync), (client, ct) => client.OpenSessionAsync(req, ct), cancellationToken);
        }

        public Task<TCloseSessionResp> CloseSessionAsync(TCloseSessionReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CloseSessionAsync), (client, ct) => client.CloseSessionAsync(req, ct), cancellationToken);
        }

        public Task<TGetInfoResp> GetInfoAsync(TGetInfoReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetInfoAsync), (client, ct) => client.GetInfoAsync(req, ct), cancellationToken);
        }

        public Task<TExecuteStatementResp> ExecuteStatementAsync(TExecuteStatementReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(ExecuteStatementAsync), (client, ct) => client.ExecuteStatementAsync(req, ct), cancellationToken);
        }

        public Task<TGetTypeInfoResp> GetTypeInfoAsync(TGetTypeInfoReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTypeInfoAsync), (client, ct) => client.GetTypeInfoAsync(req, ct), cancellationToken);
        }

        public Task<TGetCatalogsResp> GetCatalogsAsync(TGetCatalogsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetCatalogsAsync), (client, ct) => client.GetCatalogsAsync(req, ct), cancellationToken);
        }

        public Task<TGetSchemasResp> GetSchemasAsync(TGetSchemasReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetSchemasAsync), (client, ct) => client.GetSchemasAsync(req, ct), cancellationToken);
        }

        public Task<TGetTablesResp> GetTablesAsync(TGetTablesReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTablesAsync), (client, ct) => client.GetTablesAsync(req, ct), cancellationToken);
        }

        public Task<TGetTableTypesResp> GetTableTypesAsync(TGetTableTypesReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTableTypesAsync), (client, ct) => client.GetTableTypesAsync(req, ct), cancellationToken);
        }

        public Task<TGetColumnsResp> GetColumnsAsync(TGetColumnsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetColumnsAsync), (client, ct) => client.GetColumnsAsync(req, ct), cancellationToken);
        }

        public Task<TGetFunctionsResp> GetFunctionsAsync(TGetFunctionsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetFunctionsAsync), (client, ct) => client.GetFunctionsAsync(req, ct), cancellationToken);
        }

        public Task<TGetPrimaryKeysResp> GetPrimaryKeysAsync(TGetPrimaryKeysReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetPrimaryKeysAsync), (client, ct) => client.GetPrimaryKeysAsync(req, ct), cancellationToken);
        }

        public Task<TGetCrossReferenceResp> GetCrossReferenceAsync(TGetCrossReferenceReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetCrossReferenceAsync), (client, ct) => client.GetCrossReferenceAsync(req, ct), cancellationToken);
        }

        public Task<TGetOperationStatusResp> GetOperationStatusAsync(TGetOperationStatusReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetOperationStatusAsync), (client, ct) => client.GetOperationStatusAsync(req, ct), cancellationToken);
        }

        public Task<TCancelOperationResp> CancelOperationAsync(TCancelOperationReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CancelOperationAsync), (client, ct) => client.CancelOperationAsync(req, ct), cancellationToken);
        }

        public Task<TCloseOperationResp> CloseOperationAsync(TCloseOperationReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CloseOperationAsync), (client, ct) => client.CloseOperationAsync(req, ct), cancellationToken);
        }

        public Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetResultSetMetadataReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetResultSetMetadataAsync), (client, ct) => client.GetResultSetMetadataAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TFetchResultsResp> FetchResultsAsync(TFetchResultsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(FetchResultsAsync), (client, ct) => client.FetchResultsAsync(req, ct), cancellationToken);
        }

        public Task<TGetDelegationTokenResp> GetDelegationTokenAsync(TGetDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetDelegationTokenAsync), (client, ct) => client.GetDelegationTokenAsync(req, ct), cancellationToken);
        }

        public Task<TCancelDelegationTokenResp> CancelDelegationTokenAsync(TCancelDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CancelDelegationTokenAsync), (client, ct) => client.CancelDelegationTokenAsync(req, ct), cancellationToken);
        }

        public Task<TRenewDelegationTokenResp> RenewDelegationTokenAsync(TRenewDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(RenewDelegationTokenAsync), (client, ct) => client.RenewDelegationTokenAsync(req, ct), cancellationToken);
        }
    }
}
