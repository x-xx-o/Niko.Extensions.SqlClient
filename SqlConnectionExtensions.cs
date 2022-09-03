using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Microsoft.Data.SqlClient;

public static class SqlConnectionExtensions
{
    public const int DefaultCommandTimeout = 3600;

    public static int CommandTimeout { get; set; } = DefaultCommandTimeout;

    #region Gestion des transactions

    static readonly ConditionalWeakTable<SqlConnection, SqlTransaction> transactions = new();

    /// <summary>
    /// Exécute les actions dans un contexte transactionel autorisant les niveaux multiples.
    /// </summary>
    public static async Task TransactionalAsync(this SqlConnection db, Func<SqlTransaction, Task> action, 
        IsolationLevel isolationLevel = IsolationLevel.ReadUncommitted)
    {
        if (transactions.TryGetValue(db, out var currentTx))
        {
            // Déjà dans une transaction
            await action(currentTx);
        }
        else
        {
            // Création d'une transaction

            await EnsureConnectionNotClosedAsync(db);

            using var tx = (SqlTransaction)await db.BeginTransactionAsync(isolationLevel);

            transactions.Add(db, tx);

            try
            {
                await action(tx);
            }
            finally
            {
                transactions.Remove(db);
            }
        }
    }

    #endregion

    #region Gestion des paramètres

    static SqlParameter CreateSqlParameter(string name, object? value)
    {

        var parameter = new SqlParameter(name, value ?? DBNull.Value);

        return parameter;
    }

    static IEnumerable<SqlParameter> ObjectToParameters(object obj)
    {
        if (obj is IEnumerable<KeyValuePair<string, object>> dic)
        {
            foreach (var kv in dic)
            {
                var name = kv.Key.StartsWith('@') ? kv.Key : $"@{kv.Key}";
                var value = kv.Value;

                yield return CreateSqlParameter(name, value);
            }
        }
        else
        {
            var props = TypeDescriptor.GetProperties(obj);

            foreach (PropertyDescriptor prop in props)
            {
                var name = prop.Name.StartsWith('@') ? prop.Name : $"@{prop.Name}";
                var value = prop.GetValue(obj);

                yield return CreateSqlParameter(name, value);
            }
        }
    }

    #endregion

    public static async Task EnsureConnectionNotClosedAsync(this SqlConnection db)
    {
        if (db.State == ConnectionState.Closed)
        {
            await db.OpenAsync();
        }
    }

    static SqlCommand CreateCommand(SqlConnection db, string query, object? parameters)
    {
        var command = new SqlCommand(query, db)
        {
            CommandTimeout = CommandTimeout
        };


        if (parameters != null)
        {
            foreach (var parameter in ObjectToParameters(parameters))
            {
                command.Parameters.Add(parameter);
            }
        }

        if (transactions.TryGetValue(db, out var tx))
        {
            command.Transaction = tx;
        }

        return command;
    }

    static void PopulateEntity(SqlDataReader reader, object entity)
    {
        var props = entity.GetType().GetProperties(
            BindingFlags.Public | BindingFlags.Instance);

        var fieldCount = reader.FieldCount;
        var values = new object[fieldCount];
        reader.GetValues(values);

        for (int i = 0; i < fieldCount; i++)
        {
            var name = reader.GetName(i);
            var prop = props.FirstOrDefault(x => string.Equals(name, x.Name, StringComparison.OrdinalIgnoreCase));

            if (prop != null)
            {
                var value = values[i];
                if (value is not DBNull)
                {
                    if (prop.PropertyType.IsEnum && value is string vs && Enum.TryParse(prop.PropertyType, vs, true, out var enumValue))
                    {
                        prop.SetValue(entity, enumValue);
                    }
                    else
                    {
                        prop.SetValue(entity, value);
                    }

                }
            }

        }
    }

    static TEntity ToEntity<TEntity>(this SqlDataReader reader)
    {
        var entity = Activator.CreateInstance<TEntity>();

        if (entity != null)
        {
            PopulateEntity(reader, entity);
        }

        return entity;
    }

    public static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        if (await reader.ReadAsync())
        {
            return reader.ToEntity<TEntity>();
        }

        return default;
    }

    public static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var result = new List<TEntity>();

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        while (await reader.ReadAsync())
        {
            result.Add(reader.ToEntity<TEntity>());
        }

        return result;
    }

    public static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var result = new List<object?[]>();

        using var reader = await GetDataReaderAsync(db, query, parameters);

        while (await reader.ReadAsync())
        {
            var values = new object?[reader.FieldCount];
            reader.GetValues(values);

            for (var i = 0; i < values.Length; i++)
            {
                if (values[i] is DBNull)
                {
                    values[i] = null;
                }
            }

            result.Add(values);
        }

        return result;
    }

    public static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var result = new List<object?[]>();
        var columns = new List<string>();

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        while (await reader.ReadAsync())
        {
            if (!columns.Any())
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    columns.Add(reader.GetName(i));
                }
            }

            var row = new object?[reader.FieldCount];

            for (var i = 0; i < reader.FieldCount; i++)
            {
                try
                {
                    var value = reader.GetValue(i);
                    if (value is DBNull)
                    {
                        value = null;
                    }
                    row[i] = value;
                }
                catch
                {
                    row[i] = null;
                }
            }

            result.Add(row);
        }

        return (columns, result);
    }

    public static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
        where TValue : struct
    {
        // Il faut prendre en compte la possibilité d'avoir un lot de requêtes.
        // Si tel est le cas, il faut lire toutes les réponses donc passer par un reader.

        object? obj = null;

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);
        do
        {
            while (await reader.ReadAsync())
            {
                obj = reader[0];
            }

        } while (await reader.NextResultAsync());

        return obj is TValue value ? value : default;

    }

    public static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var result = new List<TValue>();

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        while (await reader.ReadAsync())
        {
            result.Add((TValue)reader[0]);
        }

        return result;
    }

    public static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
        where TKey : notnull
        where TValue : struct
    {
        var result = new Dictionary<TKey, TValue>();

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        while (await reader.ReadAsync())
        {
            var okey = reader[0];
            var ovalue = reader[1];

            var key = okey is DBNull ? default : (TKey)okey;

            if (key != null)
            {
                var value = ovalue is DBNull ? default : (TValue)ovalue;
                result[key] = value;
            }
        }

        return result;
    }

    public static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        using var reader = await GetDataReaderAsync(db, query.Value, parameters);

        if (await reader.ReadAsync())
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var ovalue = reader[i];

                var key = reader.GetName(i);
                var value = ovalue is DBNull ? default : ovalue;

                result[key] = value;
            }
        }

        return result;
    }

    public static async Task<int> ExecuteAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query.Value, parameters);

        return await command.ExecuteNonQueryAsync();
    }

    public static async Task<TIdentity> ExecuteAndSelectIdentityAsync<TIdentity>(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        var value = await GetValueAsync<decimal>(db, query.Value + "; select @@identity;", parameters);

        if (typeof(TIdentity) == typeof(int))
        {
            return (TIdentity)(object)Convert.ToInt32(value);
        }
        else if (typeof(TIdentity) == typeof(long))
        {
            return (TIdentity)(object)Convert.ToInt64(value);
        }
        else
        {
            return (TIdentity)(object)value;
        }
    }

    public static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query.Value, parameters);

        return await command.ExecuteReaderAsync();
    }

    #region FormattableString

    static SqlCommand CreateCommand(SqlConnection db, FormattableString query)
    {
        var command = new SqlCommand()
        {
            Connection = db,
            CommandTimeout = CommandTimeout
        };

        var sql = query.Format;
        var args = query.GetArguments();

        for (int i = 0; i < args.Length; i++)
        {
            var value = args[i];

            if (value is Enum)
            {
                var type = value.GetType();
                value = Convert.ChangeType(value, type.GetEnumUnderlyingType());
            }

            if (value is IEnumerable en && value is not string)
            {
                string? values = null;

                switch (value)
                {
                    // Remplacements inline
                    case IEnumerable<byte>      e: values = string.Join(",", e); break;
                    case IEnumerable<short>     e: values = string.Join(",", e); break;
                    case IEnumerable<int>       e: values = string.Join(",", e); break;
                    case IEnumerable<long>      e: values = string.Join(",", e); break;
                    case IEnumerable<sbyte>     e: values = string.Join(",", e); break;
                    case IEnumerable<ushort>    e: values = string.Join(",", e); break;
                    case IEnumerable<uint>      e: values = string.Join(",", e); break;
                    case IEnumerable<ulong>     e: values = string.Join(",", e); break;
                    case IEnumerable<bool>      e: values = string.Join(",", e.Select(v => v ? "1" : "0")); break;
                    case IEnumerable<float>     e: values = string.Join(",", e.Select(v => v.ToString(CultureInfo.InvariantCulture))); break;
                    case IEnumerable<double>    e: values = string.Join(",", e.Select(v => v.ToString(CultureInfo.InvariantCulture))); break;
                    case IEnumerable<decimal>   e: values = string.Join(",", e.Select(v => v.ToString(CultureInfo.InvariantCulture))); break;

                    // Paramètres
                    default:
                        var j = 0;

                        var pnames = new List<string>();
                        foreach (var v in en)
                        {
                            var pname = GetParamName($"p{i}_{j++}");

                            command.Parameters.Add(CreateSqlParameter(pname, v));
                        }

                        values = string.Join(",", pnames);
                        break;
                }

                sql = sql.Replace("{" + i + "}", values);
            }
            else
            {
                var pname = GetParamName($"p{i}");
                sql = sql.Replace("{" + i + "}", pname);
                command.Parameters.Add(CreateSqlParameter(pname, value));
            }

        }

        command.CommandText = sql;

        return command;

        static string GetParamName(string undecoratedParamName) => $"@{undecoratedParamName}";
        
    }

    public static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, FormattableString query)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query);

        return await command.ExecuteReaderAsync();
    }

    public static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, FormattableString query)
    {
        using var reader = await GetDataReaderAsync(db, query);

        if (await reader.ReadAsync())
        {
            return reader.ToEntity<TEntity>();
        }

        return default;
    }

    public static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, FormattableString query)
    {
        var result = new List<TEntity>();

        using var reader = await GetDataReaderAsync(db, query);

        while (await reader.ReadAsync())
        {
            result.Add(reader.ToEntity<TEntity>());
        }

        return result;
    }

    public static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, FormattableString query)
    {
        var result = new List<object?[]>();

        using var reader = await GetDataReaderAsync(db, query);

        while (await reader.ReadAsync())
        {
            var values = new object?[reader.FieldCount];
            reader.GetValues(values);

            for (var i = 0; i < values.Length; i++)
            {
                if (values[i] is DBNull)
                {
                    values[i] = null;
                }
            }

            result.Add(values);
        }

        return result;
    }

    public static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(this SqlConnection db, FormattableString query)
    {
        var result = new List<object?[]>();
        var columns = new List<string>();

        using var reader = await GetDataReaderAsync(db, query);

        while (await reader.ReadAsync())
        {
            if (!columns.Any())
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    columns.Add(reader.GetName(i));
                }
            }

            var row = new object?[reader.FieldCount];

            for (var i = 0; i < reader.FieldCount; i++)
            {
                try
                {
                    var value = reader.GetValue(i);
                    if (value is DBNull)
                    {
                        value = null;
                    }
                    row[i] = value;
                }
                catch
                {
                    row[i] = null;
                }
            }

            result.Add(row);
        }

        return (columns, result);
    }

    public static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, FormattableString query)
        where TValue : struct
    {
        // Il faut prendre en compte la possibilité d'avoir un lot de requêtes.
        // Si tel est le cas, il faut lire toutes les réponses donc passer par un reader.

        object? obj = null;

        using var reader = await GetDataReaderAsync(db, query);
        do
        {
            while (await reader.ReadAsync())
            {
                obj = reader[0];
            }

        } while (await reader.NextResultAsync());

        return obj is TValue value ? value : default;

    }

    public static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, FormattableString query)
    {
        var result = new List<TValue>();

        using var reader = await GetDataReaderAsync(db, query);

        while (await reader.ReadAsync())
        {
            result.Add((TValue)reader[0]);
        }

        return result;
    }

    public static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, FormattableString query)
        where TKey : notnull
        where TValue : struct
    {
        var result = new Dictionary<TKey, TValue>();

        using var reader = await GetDataReaderAsync(db, query);

        while (await reader.ReadAsync())
        {
            var okey = reader[0];
            var ovalue = reader[1];

            var key = okey is DBNull ? default : (TKey)okey;

            if (key != null)
            {
                var value = ovalue is DBNull ? default : (TValue)ovalue;
                result[key] = value;
            }
        }

        return result;
    }

    public static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, FormattableString query)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        using var reader = await GetDataReaderAsync(db, query);

        if (await reader.ReadAsync())
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var ovalue = reader[i];

                var key = reader.GetName(i);
                var value = ovalue is DBNull ? default : ovalue;

                result[key] = value;
            }
        }

        return result;
    }

    public static async Task<int> ExecuteAsync(this SqlConnection db, FormattableString query)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query);

        return await command.ExecuteNonQueryAsync();
    }

    #endregion
}
