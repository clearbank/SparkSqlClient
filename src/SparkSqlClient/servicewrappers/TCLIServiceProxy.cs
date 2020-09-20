using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.generated;

namespace SparkSqlClient.servicewrappers
{
    internal abstract class TCLIServiceProxy : TCLIService.IAsync, IDisposable
    {
        protected readonly TCLIService.IAsync Inner;

        protected TCLIServiceProxy(TCLIService.IAsync inner)
        {
            Inner = inner;
        }

        public abstract Task<TResult> Proxy<TResult>(string method, Func<Task<TResult>> action, CancellationToken cancellationToken);
        

        public Task<TOpenSessionResp> OpenSessionAsync(TOpenSessionReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(OpenSessionAsync), () => Inner.OpenSessionAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TCloseSessionResp> CloseSessionAsync(TCloseSessionReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CloseSessionAsync), () => Inner.CloseSessionAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetInfoResp> GetInfoAsync(TGetInfoReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetInfoAsync), () => Inner.GetInfoAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TExecuteStatementResp> ExecuteStatementAsync(TExecuteStatementReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(ExecuteStatementAsync), () => Inner.ExecuteStatementAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetTypeInfoResp> GetTypeInfoAsync(TGetTypeInfoReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTypeInfoAsync), () => Inner.GetTypeInfoAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetCatalogsResp> GetCatalogsAsync(TGetCatalogsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetCatalogsAsync), () => Inner.GetCatalogsAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetSchemasResp> GetSchemasAsync(TGetSchemasReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetSchemasAsync), () => Inner.GetSchemasAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetTablesResp> GetTablesAsync(TGetTablesReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTablesAsync), () => Inner.GetTablesAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetTableTypesResp> GetTableTypesAsync(TGetTableTypesReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetTableTypesAsync), () => Inner.GetTableTypesAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetColumnsResp> GetColumnsAsync(TGetColumnsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetColumnsAsync), () => Inner.GetColumnsAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetFunctionsResp> GetFunctionsAsync(TGetFunctionsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetFunctionsAsync), () => Inner.GetFunctionsAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetPrimaryKeysResp> GetPrimaryKeysAsync(TGetPrimaryKeysReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetPrimaryKeysAsync), () => Inner.GetPrimaryKeysAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetCrossReferenceResp> GetCrossReferenceAsync(TGetCrossReferenceReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetCrossReferenceAsync), () => Inner.GetCrossReferenceAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetOperationStatusResp> GetOperationStatusAsync(TGetOperationStatusReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetOperationStatusAsync), () => Inner.GetOperationStatusAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TCancelOperationResp> CancelOperationAsync(TCancelOperationReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CancelOperationAsync), () => Inner.CancelOperationAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TCloseOperationResp> CloseOperationAsync(TCloseOperationReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CloseOperationAsync), () => Inner.CloseOperationAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetResultSetMetadataReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetResultSetMetadataAsync), () => Inner.GetResultSetMetadataAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TFetchResultsResp> FetchResultsAsync(TFetchResultsReq req, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(FetchResultsAsync), () => Inner.FetchResultsAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TGetDelegationTokenResp> GetDelegationTokenAsync(TGetDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(GetDelegationTokenAsync), () => Inner.GetDelegationTokenAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TCancelDelegationTokenResp> CancelDelegationTokenAsync(TCancelDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(CancelDelegationTokenAsync), () => Inner.CancelDelegationTokenAsync(req, cancellationToken), cancellationToken);
        }

        public Task<TRenewDelegationTokenResp> RenewDelegationTokenAsync(TRenewDelegationTokenReq req,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Proxy(nameof(RenewDelegationTokenAsync), () => Inner.RenewDelegationTokenAsync(req, cancellationToken), cancellationToken);
        }

        public void Dispose()
        {
            (Inner as IDisposable)?.Dispose();
        }
    }
}
