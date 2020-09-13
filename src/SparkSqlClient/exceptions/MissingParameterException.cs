using System;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when a parameter is referenced but has not been defined.
    /// </summary>
    /// <seealso cref="System.Exception" />
    public class MissingParameterException : Exception
    {
        /// <summary>
        /// Gets the name of the parameter that is missing.
        /// </summary>
        public string ParameterName { get; }

        internal MissingParameterException(string parameterName)
        :base($"Parameter '{parameterName}' in CommandText is not defined.")
        {
            ParameterName = parameterName;
        }
    }
}
