using System;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;

namespace SparkSqlClient
{
    internal class SparkParameterCollection : DbParameterCollection
    {
        protected Collection<SparkParameter> Parameters { get; } = new Collection<SparkParameter>();

        private SparkParameter AsSparkParamter(object value, string parameterName)
        {
            
            return value as SparkParameter
                        ?? throw new ArgumentException($"{parameterName} must be of type {typeof(SparkParameter)}", parameterName);
        }

        private int InternalIndexOf(string parameterName)
        {
            return Parameters
                .Select((sparkParameter, index) => new {sparkParameter, index})
                .FirstOrDefault(x => x.sparkParameter.ParameterName == parameterName)
                ?.index ?? -1;
        }

        public override int Add(object value)
        {
            lock (SyncRoot)
            {
                Parameters.Add(AsSparkParamter(value, nameof(value)));
                return 1;
            }
        }

        public override void Clear()
        {
            lock (SyncRoot)
            {
                Parameters.Clear();
            }
        }

        public override bool Contains(object value)
        {
            lock (SyncRoot)
            {
                return Parameters.Contains(AsSparkParamter(value, nameof(value)));
            }
        }

        public override int IndexOf(object value)
        {
            lock (SyncRoot)
            {
                return Parameters.IndexOf(AsSparkParamter(value, nameof(value)));
            }
        }

        public override void Insert(int index, object value)
        {
            lock (SyncRoot)
            {
                Parameters.Insert(index, AsSparkParamter(value, nameof(value)));
            }
        }

        public override void Remove(object value)
        {
            lock (SyncRoot)
            {
                Parameters.Remove(AsSparkParamter(value, nameof(value)));
            }
        }

        public override void RemoveAt(int index)
        {
            lock (SyncRoot)
            {
                Parameters.RemoveAt(index);
            }
        }

        public override void RemoveAt(string parameterName)
        {
            lock (SyncRoot)
            {
                var index = InternalIndexOf(parameterName);
                if (index >= 0)
                    Parameters.RemoveAt(index);
            }
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            lock (SyncRoot)
            {
                Parameters[index] = AsSparkParamter(value, nameof(value));
            }
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            lock (SyncRoot)
            {
                var index = InternalIndexOf(parameterName);
                if (index >= 0)
                    Parameters[index] = AsSparkParamter(value, nameof(value));
            }
        }

        public override int Count => Parameters.Count;
        public override object SyncRoot { get; } = new object();

        public override int IndexOf(string parameterName)
        {
            lock (SyncRoot)
            {
                return InternalIndexOf(parameterName);
            }
        }

        public override bool Contains(string value)
        {
            lock (SyncRoot)
            {
                return InternalIndexOf(value) >= 0;
            }
        }

        public override void CopyTo(Array array, int index)
        {
            if (array == null) throw new ArgumentException(nameof(array));
            lock (SyncRoot)
            {
                ((ICollection)Parameters).CopyTo(array, index);
            }
        }

        public override IEnumerator GetEnumerator()
        {
            lock (SyncRoot)
            {
                return new Collection<SparkParameter>(Parameters).GetEnumerator();
            }
        }

        protected override DbParameter GetParameter(int index)
        {
            lock (SyncRoot)
            {
                return Parameters[index];
            }
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            lock (SyncRoot)
            {
                var index = InternalIndexOf(parameterName);
                return Parameters[index];
            }
        }

        public override void AddRange(Array values)
        {
            lock (SyncRoot)
            {
                foreach (var sparkParameter in values.OfType<DbParameter>().Select(x => AsSparkParamter(x, nameof(values))))
                {
                    Parameters.Add(sparkParameter);
                }
            }
        }
    }
}
