using System;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when the requested data type does not match the columns native data type.
    /// </summary>
    /// <seealso cref="System.InvalidCastException" />
    public class InvalidDataTypeException 
        : InvalidCastException // Extending InvalidCastException to conform with data provider interfaces
    {
        /// <summary>
        /// Gets the ordinal location of the column.
        /// </summary>
        public int Ordinal { get; }

        /// <summary>
        /// Gets the spark type the column contains.
        /// </summary>
        public string ActualTypeName { get; }

        /// <summary>
        /// Gets the type that the column contains.
        /// </summary>
        public Type ActualType { get; }

        /// <summary>
        /// Gets the type that was requested for the column.
        /// </summary>
        public Type RequestedType { get; }

        internal InvalidDataTypeException(int ordinal, string actualTypeName, Type actualType, Type requestedType)
            :base($"Unable to read type '{requestedType}' from spark type '{actualTypeName}' in column {ordinal}. Try reading column as type '{actualType}'.")
        {
            Ordinal = ordinal;
            ActualTypeName = actualTypeName;
            ActualType = actualType;
            RequestedType = requestedType;
        }

        internal InvalidDataTypeException(int ordinal, string actualTypeName, Type requestedType)
            : base($"Unable to read type '{requestedType}' from spark type '{actualTypeName}' in column {ordinal}. Spark type '{actualTypeName}' is not supported.")
        {
            Ordinal = ordinal;
            ActualTypeName = actualTypeName;
            ActualType = null;
            RequestedType = requestedType;
        }
    }
}
