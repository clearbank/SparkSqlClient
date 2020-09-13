using System.Data.Common;
using System.Linq;

namespace SparkSqlClient
{
    /// <summary>
    /// Represents a connection string to connect to a spark server.
    /// </summary>
    /// <seealso cref="System.Data.Common.DbConnectionStringBuilder" />
    public class SparkConnectionStringBuilder : DbConnectionStringBuilder
    {
        private static readonly string[] UserIdKeys = new [] { "User ID" };
        private static readonly string[] PasswordKeys = new [] { "Password", "Pwd" };
        private static readonly string[] DataSourceKeys = new [] {"Data Source" };

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkConnectionStringBuilder"/> class.
        /// </summary>
        public SparkConnectionStringBuilder() : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SparkConnectionStringBuilder"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public SparkConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Gets or sets the username.
        /// </summary>
        public string UserId
        {
            get => UserIdKeys
                .Select(x => TryGetValue(x, out object valueObj) ? valueObj as string : null)
                .FirstOrDefault(x => x != null);
            set => Add(UserIdKeys.First(), value);
        }

        /// <summary>
        /// Gets or sets the password.
        /// </summary>
        public string Password
        {
            get => PasswordKeys
                .Select(x => TryGetValue(x, out object valueObj) ? valueObj as string : null)
                .FirstOrDefault(x => x != null);
            set => Add(PasswordKeys.First(), value);
        }

        /// <summary>
        /// Gets or sets the Data Source. This should be the full URI to the spark http server, including the path.
        /// </summary>
        public string DataSource
        {
            get => DataSourceKeys
                .Select(x => TryGetValue(x, out object valueObj) ? valueObj as string : null)
                .FirstOrDefault(x => x != null);
            set => Add(DataSourceKeys.First(), value);
        }
    }
}
