using System;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.generated;

namespace SparkSqlClient.servicewrappers
{
    /// <summary>
    /// Represents a TCLIService.IAsync that synchronizes all methods. The default generated
    /// templates do not support concurrent access, this class ensures this failing does not
    /// cause errors
    /// </summary>
    /// <seealso cref="SparkSqlClient.servicewrappers.TCLIServiceProxy" />
    internal class TCLIServiceSync : TCLIServiceProxy
    {
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        public TCLIServiceSync(TCLIService.IAsync inner) : base(inner)
        {
        }

        public override async Task<TResult> Proxy<TResult>(string method, Func<Task<TResult>> action, CancellationToken cancellationToken = default(CancellationToken))
        {
            await _semaphoreSlim.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await action();
            }
            finally
            {
                _semaphoreSlim.Release();
            }
        }
    }
}