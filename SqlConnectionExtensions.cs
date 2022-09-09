using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;

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

    static IEnumerable<SqlParameter> ObjectToParameters(object? obj)
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
        else if (obj is IEnumerable<SqlParameter> alreayConverted)
        {
            foreach (var p in alreayConverted)
            {
                yield return p;
            }
        }
        else if (obj != null)
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

    #region Base

    public static async Task EnsureConnectionNotClosedAsync(this SqlConnection db)
    {
        if (db.State == ConnectionState.Closed)
        {
            await db.OpenAsync();
        }
    }

    static SqlCommand CreateCommand(SqlConnection db, SqlQuery sqlQuery)
    {
        var command = new SqlCommand(sqlQuery.QueryString, db)
        {
            CommandTimeout = CommandTimeout
        };

        if (sqlQuery.Parameters != null && sqlQuery.Parameters.Any())
        {
            command.Parameters.AddRange(sqlQuery.Parameters.ToArray());
        }

        if (transactions.TryGetValue(db, out var tx))
        {
            command.Transaction = tx;
        }

        return command;
    }

    #endregion

    #region Matérialisation

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
                    // Enum string
                    if (prop.PropertyType.IsEnum 
                        && value is string vs 
                        && Enum.TryParse(prop.PropertyType, vs, true, out var enumValue))
                    {
                        prop.SetValue(entity, enumValue);
                    }
                    // Objet ou tableau JSON
                    else if (prop.PropertyType.IsClass 
                        && prop.PropertyType != typeof(string)
                        && value is string s 
                        && (s.StartsWith('{') || s.StartsWith('[')))
                    {
                        var deserialized = JsonSerializer.Deserialize(s, prop.PropertyType);
                        prop.SetValue(entity, deserialized);
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

    #endregion

    #region Exécution des requêtes

    static SqlQuery RawSqlStringToSqlQuery(RawSqlString query, object? parameters)
    {
        return new SqlQuery
        {
            QueryString = query.Value,
            Parameters = ObjectToParameters(parameters).ToList()
        };
    }

    static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, SqlQuery sqlQuery)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, sqlQuery);

        return await command.ExecuteReaderAsync();
    }

    static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, SqlQuery sqlQuery)
    {
        using var reader = await GetDataReaderAsync(db, sqlQuery);

        if (await reader.ReadAsync())
        {
            return reader.ToEntity<TEntity>();
        }

        return default;
    }

    static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, SqlQuery sqlQuery)
    {
        var result = new List<TEntity>();

        using var reader = await GetDataReaderAsync(db, sqlQuery);

        while (await reader.ReadAsync())
        {
            result.Add(reader.ToEntity<TEntity>());
        }

        return result;
    }

    static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, SqlQuery sqlQuery)
    {
        var result = new List<object?[]>();

        using var reader = await GetDataReaderAsync(db, sqlQuery);

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

    static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(this SqlConnection db, SqlQuery sqlQuery)
    {
        var result = new List<object?[]>();
        var columns = new List<string>();

        using var reader = await GetDataReaderAsync(db, sqlQuery);

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

    static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, SqlQuery sqlQuery)
        where TValue : struct
    {
        // Il faut prendre en compte la possibilité d'avoir un lot de requêtes.
        // Si tel est le cas, il faut lire toutes les réponses donc passer par un reader.

        object? obj = null;

        using var reader = await GetDataReaderAsync(db, sqlQuery);
        do
        {
            while (await reader.ReadAsync())
            {
                obj = reader[0];
            }

        } while (await reader.NextResultAsync());

        return obj is TValue value ? value : default;

    }

    static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, SqlQuery sqlQuery)
    {
        var result = new List<TValue>();

        using var reader = await GetDataReaderAsync(db, sqlQuery);

        while (await reader.ReadAsync())
        {
            result.Add((TValue)reader[0]);
        }

        return result;
    }

    static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, SqlQuery sqlQuery)
        where TKey : notnull
        where TValue : struct
    {
        var result = new Dictionary<TKey, TValue>();

        using var reader = await GetDataReaderAsync(db, sqlQuery);

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

    static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, SqlQuery sqlQuery)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        using var reader = await GetDataReaderAsync(db, sqlQuery);

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

    static async Task<int> ExecuteAsync(this SqlConnection db, SqlQuery sqlQuery)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, sqlQuery);

        return await command.ExecuteNonQueryAsync();
    }

    static async Task<TIdentity> ExecuteAndSelectIdentityAsync<TIdentity>(this SqlConnection db, SqlQuery sqlQuery)
    {
        var newQuery = new SqlQuery
        {
            QueryString = sqlQuery.QueryString + "; select @@identity;",
            Parameters = sqlQuery.Parameters
        };

        var value = await GetValueAsync<decimal>(db, newQuery);

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

    #endregion

    #region RawSqlString

    public static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetEntityAsync<TEntity>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetEntityListAsync<TEntity>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetRawDataListAsync(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetRawDataListWithColumnNamesAsync(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
        where TValue : struct
        => await GetValueAsync<TValue>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetListAsync<TValue>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, RawSqlString query, object? parameters = null)
        where TKey : notnull
        where TValue : struct
        => await GetDictionaryAsync<TKey, TValue>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetObjectAsDictionaryAsync(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<int> ExecuteAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await ExecuteAsync(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<TIdentity> ExecuteAndSelectIdentityAsync<TIdentity>(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await ExecuteAndSelectIdentityAsync<TIdentity>(db, RawSqlStringToSqlQuery(query, parameters));

    public static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, RawSqlString query, object? parameters = null)
        => await GetDataReaderAsync(db, RawSqlStringToSqlQuery(query, parameters));

    #endregion

    #region FormattableString

    static SqlQuery FormattableStringToSqlQuery(FormattableString query)
    {
        var sql = query.Format;
        var args = query.GetArguments();

        var parameters = new List<SqlParameter>();

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

                            parameters.Add(CreateSqlParameter(pname, v));
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
                parameters.Add(CreateSqlParameter(pname, value));
            }

        }



        return new SqlQuery
        {
            QueryString = sql,
            Parameters = parameters
        };

        static string GetParamName(string undecoratedParamName) => $"@{undecoratedParamName}";
        
    }

    public static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, FormattableString query)
        => await GetDataReaderAsync(db, FormattableStringToSqlQuery(query));

    public static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, FormattableString query)
        => await GetEntityAsync<TEntity>(db, FormattableStringToSqlQuery(query));

    public static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, FormattableString query)
        => await GetEntityListAsync<TEntity>(db, FormattableStringToSqlQuery(query));

    public static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, FormattableString query)
        => await GetRawDataListAsync(db, FormattableStringToSqlQuery(query));

    public static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(this SqlConnection db, FormattableString query)
        => await GetRawDataListWithColumnNamesAsync(db, FormattableStringToSqlQuery(query));

    public static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, FormattableString query)
        where TValue : struct
        => await GetValueAsync<TValue>(db, FormattableStringToSqlQuery(query));

    public static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, FormattableString query)
        => await GetListAsync<TValue>(db, FormattableStringToSqlQuery(query));

    public static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, FormattableString query)
        where TKey : notnull
        where TValue : struct
        => await GetDictionaryAsync<TKey, TValue>(db, FormattableStringToSqlQuery(query));

    public static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, FormattableString query)
        => await GetObjectAsDictionaryAsync(db, FormattableStringToSqlQuery(query));

    public static async Task<int> ExecuteAsync(this SqlConnection db, FormattableString query)
        => await ExecuteAsync(db, FormattableStringToSqlQuery(query));

    #endregion
}
