# Spark SQL Client

Spark SQL Client implements a native ADO.NET connection to a spark thrift server. Allowing dotnet applications to make spark SQL queries without requiring 3rd party ODBC drivers.


## Raw ADO.NET example

`SparkConnection` implements DbConnection and can be used in the same way as any other database connection in [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-code-examples)

```csharp
await using var conn = new SparkConnection("Data Source=https://mydomain.net/path/to/thrift/server; User ID=myusername; Password=pa55w0rd");
conn.Open();

using DbDataReader reader = await conn.ExecuteReaderAsync("SELECT ID, Description FROM Entities");

reader.Read();
var id = reader.GetValue("ID");
var description = reader.GetValue("Description");
```

## Dapper example
`SparkConnection` supports the [dapper](https://github.com/StackExchange/Dapper) library, allowing a simpler interface over the raw ADO.NET

```csharp
await using var conn = new SparkConnection("Data Source=https://mydomain.net/path/to/thrift/server; User ID=myusername; Password=pa55w0rd");

IEnumerable<DapperTests> entities = await conn.QueryAsync<DapperTests>("SELECT ID, Description FROM Entities");

```

## Connection String
A `SparkConnection` requires a connection. The current string supports the following properties
* `Data Source` (Required) - The full URL for the spark server
* `User ID` (Required) - The username to use for authentication
* `Password` (Required) - The password to use for authentication

### Databricks
To build  a connection string for spark running within [databricks](https://databricks.com/)

### Data Source
1. launch the databricks workspace
1. click on the Clusters icon
1. click a cluster
1. expand "Advanced Options"
1. click the JDBC/ODBC tab
1. set the connection string `Data Source` as `https://<server-hostname>/<http-path>` (It should look something like https://adb-1556877622322125.5.azuredatabricks.net/sql/protocolv1/o/1556877622322125/0207-135143-scoot967)

### Authentication
1. launch the databricks workspace
1. click on your username and click on user settings
1. click "Generate new token"
1. fill in details and click "Generate"
1. set connection string `Password` to the token value
1. set connection string `User ID` to `token`

Your final connection string should like similiar to
`Data Source=https://adb-1556877622322125.5.azuredatabricks.net/sql/protocolv1/o/1556877622322125/0207-135143-scoot967; User ID=token; Password=dapi62e4563e092a3a573e034339fbab013d`

## Alternatives
Connection to spark thrift servers can be made via [Simba spark jdbc odbc drivers](https://www.simba.com/drivers/spark-jdbc-odbc/). However installing ODBC drivers to use within dotnet can be difficult if you are not in control of the underlaying servers. The Spark SQL Client allows connection with requiring additional ODBC drivers.