using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using SparkSqlClient.generated;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when the spark server returns an error
    /// </summary>
    /// <seealso cref="System.Data.Common.DbException" />
    public class SparkOperationException 
        : DbException // Extending DbException to conform with data provider interfaces
    {
        /// <summary>
        /// Gets the information messages contained in the spark response.
        /// </summary>
        public IReadOnlyCollection<string> InfoMessages { get; }

        internal SparkOperationException(string message, IEnumerable<string> infoMessages) : base(message)
        {
            InfoMessages = infoMessages.ToList().AsReadOnly();
        }
        
        internal static void ThrowIfInvalidStatus(TStatus status)
        {
            if (new[] { TStatusCode.ERROR_STATUS, TStatusCode.INVALID_HANDLE_STATUS }.Contains(status.StatusCode))
                throw new SparkOperationException(status.ErrorMessage, status.InfoMessages);
        }
    }
}
