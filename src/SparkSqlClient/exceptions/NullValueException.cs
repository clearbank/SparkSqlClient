using System;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when retrieving a value from a column that contains null.
    /// </summary>
    /// <seealso cref="System.InvalidCastException" />
    public class NullValueException 
        : InvalidCastException // Extending InvalidCastException to conform with data provider interfaces
    {
        /// <summary>
        /// Gets the ordinal location of the column.
        /// </summary>
        public int Ordinal { get; }

        /// <summary>
        /// Gets the type that was requested for the column.
        /// </summary>
        public Type RequestedType { get; }

        internal NullValueException(int ordinal, Type expectedType)
        : base($"Unable to convert NULL to expectedType '{expectedType}' in column {ordinal}.")
        {
            Ordinal = ordinal;
            RequestedType = expectedType;
        }
    }
}
