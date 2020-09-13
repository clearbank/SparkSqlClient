using System;
using System.Collections.ObjectModel;
using System.Linq;

namespace SparkSqlClient.exceptions
{
    /// <summary>
    /// Exception thrown when a referenced column does not exist.
    /// </summary>
    /// <seealso cref="System.Exception" />
    public class InvalidColumnNameException : Exception
    {
        /// <summary>
        /// Gets the column name requested that resulted in the exception.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the available column names.
        /// </summary>
        public ReadOnlyCollection<string> AvailableColumnNames { get; }

        private static string PrettyPrintList(string seperator, ReadOnlyCollection<string> items, string lastItemSeperater)
        {
            return string.Join(seperator, items.Take(items.Count - 1)) + (items.Count <= 1 ? "" : lastItemSeperater) + items.LastOrDefault();
        }
        internal InvalidColumnNameException(string columnName, ReadOnlyCollection<string> availableColumnNames)
            : base($"Invalid column name '{columnName}'. Expected column name to be one of '{PrettyPrintList("', '", availableColumnNames, "' or '")}'.")
        {
            ColumnName = columnName;
            AvailableColumnNames = availableColumnNames;
        }
    }
}
