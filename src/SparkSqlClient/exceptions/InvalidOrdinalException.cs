using System;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when the requested column index does not exist
    /// </summary>
    /// <seealso cref="System.Exception" />
    public class InvalidOrdinalException : Exception
    {

        /// <summary>
        /// Gets the ordinal requested that resulted in the exception.
        /// </summary>
        public int Ordinal { get; }

        /// <summary>
        /// Gets the total number of columns that exist.
        /// </summary>
        public int ColumnCount { get; }

        internal InvalidOrdinalException(int ordinal, int columnCount)
        : base($"Invalid column number {ordinal}. Expected {ordinal} to be less than {columnCount}.")
        {
            Ordinal = ordinal;
            ColumnCount = columnCount;
        }
    }
}
