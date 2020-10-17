using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SparkSqlClient.exceptions;
using SparkSqlClient.generated;

namespace SparkSqlClient
{
    internal class SparkDataReader : DbDataReader
    {
        protected abstract class ColumnMetadata
        {
            public string ColumnName { get; }
            public string TypeName { get; }
            public Type Type { get; }
            public Func<object> Read { get; }

            public ColumnMetadata(string columnName, string typeName, Type type, Func<object> read)
            {
                ColumnName = columnName;
                TypeName = typeName;
                Type = type;
                Read = read;
            }
        }
        protected class UnsupportedColumnMetadata : ColumnMetadata
        {
            public UnsupportedColumnMetadata(string columnName, string typeName)
                : base(columnName, typeName, null, () => throw new NotSupportedException($"Unsupported column type '{typeName}'"))
            {

            }
        }

        protected class ColumnMetadata<T> : ColumnMetadata
        {
            public Func<T> TypedRead { get; }

            public ColumnMetadata(string columnName, string typeName, Func<T> typedRead)
                : base(columnName, typeName, typeof(T), ()=>typedRead())
            {
                TypedRead = typedRead;
            }
        }


        protected TCLIService.IAsync Client { get; }
        protected TOperationHandle OperationHandle { get; }
        protected Func<Task> OnClose { get; }

        protected IReadOnlyList<ColumnMetadata> Metadata { get; }

        protected TRowSet page = new TRowSet();
        protected int pageSize = 0;
        protected bool hasMoreRows = true;
        protected int currentRowOffset = 0;
        protected IReadOnlyList<BitArray> columnNullBitArray;


        public SparkDataReader(TCLIService.IAsync client, TSessionHandle sessionHandle, TOperationHandle operationHandle, TTableSchema schema, Func<Task> onClose)
        {
            Client = client;
            OperationHandle = operationHandle;
            OnClose = onClose;

            Metadata = BuildColumnMetadata(schema);
        }

        private IReadOnlyList<ColumnMetadata> BuildColumnMetadata(TTableSchema schema)
        {
            return schema.Columns.Select<TColumnDesc, ColumnMetadata>((colSchema, i) =>
            {
                // Response has a list of items but always seems to only have single entry
                // Adding safety check to ensure there is a single item
                var types = colSchema.TypeDesc.Types;
                var type = types.Take(2).ToArray();
                var singleTypeOrDefault = (type.Length == 1) ? type[0] : default;

                // Despite multiple options in the schema, only PrimitiveEntry seems to be populated
                // Adding safety checks to confirm PrimitiveEntry is populated
                var typeId = singleTypeOrDefault?.PrimitiveEntry?.Type;

                switch (typeId)
                {
                    case TTypeId.BOOLEAN_TYPE:
                        return new ColumnMetadata<bool>(colSchema.ColumnName, "boolean", () => page.Columns[i].BoolVal.Values[currentRowOffset]);
                    case TTypeId.TINYINT_TYPE:
                        return new ColumnMetadata<byte>(colSchema.ColumnName, "tinyint", () => (byte)page.Columns[i].ByteVal.Values[currentRowOffset]);
                    case TTypeId.SMALLINT_TYPE:
                        return new ColumnMetadata<Int16>(colSchema.ColumnName, "smallint", () => page.Columns[i].I16Val.Values[currentRowOffset]);
                    case TTypeId.INT_TYPE:
                        return new ColumnMetadata<int>(colSchema.ColumnName, "int", () => page.Columns[i].I32Val.Values[currentRowOffset]);
                    case TTypeId.BIGINT_TYPE:
                        return new ColumnMetadata<long>(colSchema.ColumnName, "bigint", () => page.Columns[i].I64Val.Values[currentRowOffset]);
                    case TTypeId.FLOAT_TYPE:
                        return new ColumnMetadata<float>(colSchema.ColumnName, "float", () => (float)page.Columns[i].DoubleVal.Values[currentRowOffset]);
                    case TTypeId.DOUBLE_TYPE:
                        return new ColumnMetadata<double>(colSchema.ColumnName, "double", () => page.Columns[i].DoubleVal.Values[currentRowOffset]);
                    case TTypeId.STRING_TYPE:
                        return new ColumnMetadata<string>(colSchema.ColumnName, "string", () => page.Columns[i].StringVal.Values[currentRowOffset]);
                    case TTypeId.TIMESTAMP_TYPE:
                        return new ColumnMetadata<DateTime>(colSchema.ColumnName, "timestamp", () => {
                            var stringValue = page.Columns[i].StringVal.Values[currentRowOffset];
                            return DateTime.Parse(stringValue);
                        });
                    case TTypeId.DECIMAL_TYPE:
                        return new ColumnMetadata<decimal>(colSchema.ColumnName, "decimal", () => {
                            var stringValue = page.Columns[i].StringVal.Values[currentRowOffset];
                            return decimal.Parse(stringValue);
                        });
                    case TTypeId.NULL_TYPE:
                        return new ColumnMetadata<object>(colSchema.ColumnName, "null", () => null);
                    case TTypeId.DATE_TYPE:
                        return new ColumnMetadata<DateTime>(colSchema.ColumnName, "date", () => {
                            var stringValue = page.Columns[i].StringVal.Values[currentRowOffset];
                            return DateTime.Parse(stringValue);
                        });
                    case TTypeId.VARCHAR_TYPE:
                        return new ColumnMetadata<string>(colSchema.ColumnName, "varchar", () => page.Columns[i].StringVal.Values[currentRowOffset]);
                    case TTypeId.BINARY_TYPE:
                        return new ColumnMetadata<byte[]>(colSchema.ColumnName, "binary", () => page.Columns[i].BinaryVal.Values[currentRowOffset]);

                    // Without metadata on their generics the best we can do for these types is return them as strings
                    case TTypeId.ARRAY_TYPE:
                        return new ColumnMetadata<string>(colSchema.ColumnName, "array", () => page.Columns[i].StringVal.Values[currentRowOffset]);
                    case TTypeId.MAP_TYPE:
                        return new ColumnMetadata<string>(colSchema.ColumnName, "map", () => page.Columns[i].StringVal.Values[currentRowOffset]);
                    case TTypeId.STRUCT_TYPE:
                        return new ColumnMetadata<string>(colSchema.ColumnName, "struct", () => page.Columns[i].StringVal.Values[currentRowOffset]);

                    // Types below I can not reproduce in spark, much less determine how to convert them. Will leave them as unsupported
                    case TTypeId.UNION_TYPE:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, "union");
                    case TTypeId.USER_DEFINED_TYPE:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, "userdefined");
                    case TTypeId.CHAR_TYPE:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, "char");
                    case TTypeId.INTERVAL_YEAR_MONTH_TYPE:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, "intervalyearmonth");
                    case TTypeId.INTERVAL_DAY_TIME_TYPE:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, "intervaldaytime");
                    default:
                        return new UnsupportedColumnMetadata(colSchema.ColumnName, TTypeId.INTERVAL_DAY_TIME_TYPE.ToString());
                }
            }).ToArray();
        }

        public override bool GetBoolean(int ordinal)
        {
            return GetAndValidateMetadataColumn<bool>(ordinal).TypedRead();
        }

        public override byte GetByte(int ordinal)
        {
            return GetAndValidateMetadataColumn<byte>(ordinal).TypedRead();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var bytes = GetAndValidateMetadataColumn<byte[]>(ordinal).TypedRead();
            var copyLen = Math.Min(bytes.Length, length);
            Buffer.BlockCopy(bytes, (int)dataOffset, buffer, bufferOffset, copyLen);
            return copyLen;
        }

        public override char GetChar(int ordinal)
        {
            return GetAndValidateMetadataColumn<char>(ordinal).TypedRead();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var chars = GetAndValidateMetadataColumn<char[]>(ordinal).TypedRead();
            var copyLen = Math.Min(chars.Length, length);
            Buffer.BlockCopy(chars, (int)dataOffset, buffer, bufferOffset, copyLen);
            return copyLen;
        }

        public override string GetDataTypeName(int columnIndex)
        {
            return GetColumnByIndex(Metadata, columnIndex).TypeName;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return GetAndValidateMetadataColumn<DateTime>(ordinal).TypedRead();
        }

        public override decimal GetDecimal(int ordinal)
        {
            return GetAndValidateMetadataColumn<decimal>(ordinal).TypedRead();
        }

        public override double GetDouble(int ordinal)
        {
            return GetAndValidateMetadataColumn<double>(ordinal).TypedRead();
        }

        public override Type GetFieldType(int columnIndex)
        {
            return GetColumnByIndex(Metadata, columnIndex).Type;
        }

        public override float GetFloat(int ordinal)
        {
            return GetAndValidateMetadataColumn<float>(ordinal).TypedRead();
        }

        public override Guid GetGuid(int ordinal)
        {
            return GetAndValidateMetadataColumn<Guid>(ordinal).TypedRead();
        }

        public override short GetInt16(int ordinal)
        {
            return GetAndValidateMetadataColumn<Int16>(ordinal).TypedRead();
        }

        public override int GetInt32(int ordinal)
        {
            return GetAndValidateMetadataColumn<int>(ordinal).TypedRead();
        }

        public override long GetInt64(int ordinal)
        {
            return GetAndValidateMetadataColumn<long>(ordinal).TypedRead();
        }

        public override string GetName(int columnIndex)
        {
            return GetColumnByIndex(Metadata, columnIndex).ColumnName;
        }

        public override int GetOrdinal(string name)
        {
            for (var i = 0; i < Metadata.Count; i++)
            {
                if (string.Equals(Metadata[i].ColumnName, name, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
            throw new InvalidColumnNameException(name, Metadata.Select(x => x.ColumnName).ToList().AsReadOnly());
        }

        public override string GetString(int ordinal)
        {
            return GetAndValidateMetadataColumn<string>(ordinal).TypedRead();
        }

        public override object GetValue(int ordinal)
        {
            if (IsDBNull(ordinal)) return DBNull.Value;
            return GetColumnByIndex(Metadata, ordinal).Read();
        }

        public override int GetValues(object[] values)
        {
            var copyLen = Math.Min(FieldCount, values.Length);
            for (var i = 0; i < copyLen; i++)
            {
                values[i] = GetValue(i);
            }

            return copyLen;
        }

        public override bool IsDBNull(int ordinal)
        {
            var nullBitArray = GetColumnByIndex(columnNullBitArray, ordinal);
            return currentRowOffset < nullBitArray.Count && nullBitArray.Get(currentRowOffset);
        }

        public override int FieldCount => Metadata.Count;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int RecordsAffected { get; } = -1;

        private bool? _hasRows = null;
        public override bool HasRows => _hasRows ?? true;

        private bool _isClosed = false;
        public override bool IsClosed => _isClosed;

        public override bool NextResult()
        {
            return false;
        }
        
        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            if (IsClosed)
                throw new InvalidOperationException($"Unable to read from a closed DataReader");

            // We have enough in our page to move next without fetching more rows
            if (++currentRowOffset < pageSize)
                return true;

            // We have no more rows to fetch, we are at the end
            if (!hasMoreRows)
                return false;

            // Fetch the next set of rows
            var result = await Client.FetchResultsAsync(new TFetchResultsReq()
            {
                OperationHandle = OperationHandle,
                Orientation = TFetchOrientation.FETCH_NEXT,
                MaxRows = int.MaxValue,
            }, cancellationToken).ConfigureAwait(false);
            SparkOperationException.ThrowIfInvalidStatus(result.Status);
            
            var firstColumn = result.Results.Columns[0];

            page = result.Results;
            hasMoreRows = result.HasMoreRows;
            currentRowOffset = 0;
            pageSize = new[]
            {
                firstColumn.I32Val?.Values?.Count ?? 0,
                firstColumn.BinaryVal?.Values?.Count ?? 0,
                firstColumn.BoolVal?.Values?.Count ?? 0,
                firstColumn.ByteVal?.Values?.Count ?? 0,
                firstColumn.DoubleVal?.Values?.Count ?? 0,
                firstColumn.I16Val?.Values?.Count ?? 0,
                firstColumn.I64Val?.Values?.Count ?? 0,
                firstColumn.StringVal?.Values?.Count ?? 0,
            }.Max();

            columnNullBitArray = result.Results.Columns.Select(col => new BitArray(
                col.I32Val?.Nulls
                ?? col.BinaryVal?.Nulls
                ?? col.BoolVal?.Nulls
                ?? col.ByteVal?.Nulls
                ?? col.DoubleVal?.Nulls
                ?? col.I16Val?.Nulls
                ?? col.I64Val?.Nulls
                ?? col.StringVal?.Nulls
                ?? new byte[0]
            )).ToArray();

            _hasRows ??= pageSize > 0;
            return pageSize > 0;
        }

        public override int Depth => 0;

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        public override void Close()
        {
            CloseAsync().GetAwaiter().GetResult();
        }

        public override Task CloseAsync()
        {
            _isClosed = true;
            return OnClose?.Invoke() ?? Task.CompletedTask;
        }



        private static T GetColumnByIndex<T>(IReadOnlyList<T> columns, int ordinal)
        {
            if (ordinal < 0 || ordinal >= columns.Count)
                throw new InvalidOrdinalException(ordinal, columns.Count);
            return columns[ordinal];
        }


        private ColumnMetadata<T> GetAndValidateMetadataColumn<T>(int ordinal)
        {
            var metadata = GetColumnByIndex(Metadata, ordinal);
            if (metadata.Type != typeof(T))
            {
                throw metadata.Type == null
                    ? new InvalidDataTypeException(ordinal, metadata.TypeName, typeof(T))
                    : new InvalidDataTypeException(ordinal, metadata.TypeName, metadata.Type, typeof(T));
            }

            if (columnNullBitArray[ordinal].Get(currentRowOffset))
                throw new NullValueException(ordinal, typeof(T));

            return (ColumnMetadata<T>)metadata;
        }
    }
}
