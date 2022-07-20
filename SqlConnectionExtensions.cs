using System.ComponentModel;
using System.Data;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Microsoft.Data.SqlClient;

public static class SqlConnectionExtensions
{
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
            CommandTimeout = 3600
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

    public static async Task<TEntity?> GetEntityAsync<TEntity>(this SqlConnection db, string query, object? parameters = null)
    {
        using var reader = await GetDataReaderAsync(db, query, parameters);

        if (await reader.ReadAsync())
        {
            return reader.ToEntity<TEntity>();
        }

        return default;
    }

    public static async Task<List<TEntity>> GetEntityListAsync<TEntity>(this SqlConnection db, string query, object? parameters = null)
    {
        var result = new List<TEntity>();

        using var reader = await GetDataReaderAsync(db, query, parameters);

        while (await reader.ReadAsync())
        {
            result.Add(reader.ToEntity<TEntity>());
        }

        return result;
    }

    public static async Task<List<object?[]>> GetRawDataListAsync(this SqlConnection db, string query, object? parameters = null)
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

    public static async Task<(List<string>, List<object?[]>)> GetRawDataListWithColumnNamesAsync(
        this SqlConnection db, string query, object? parameters = null)
    {
        var result = new List<object?[]>();
        var columns = new List<string>();

        using var reader = await GetDataReaderAsync(db, query, parameters);

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

    public static async Task<TValue> GetValueAsync<TValue>(this SqlConnection db, string query, object? parameters = null)
        where TValue : struct
    {
        // Il faut prendre en compte la possibilité d'avoir un lot de requêtes.
        // Si tel est le cas, il faut lire toutes les réponses donc passer par un reader.

        object? obj = null;

        using var reader = await GetDataReaderAsync(db, query, parameters);
        do
        {
            while (await reader.ReadAsync())
            {
                obj = reader[0];
            }

        } while (await reader.NextResultAsync());

        return obj is TValue value ? value : default;

    }

    public static async Task<List<TValue>> GetListAsync<TValue>(this SqlConnection db, string query, object? parameters = null)
    {
        var result = new List<TValue>();

        using var reader = await GetDataReaderAsync(db, query, parameters);

        while (await reader.ReadAsync())
        {
            result.Add((TValue)reader[0]);
        }

        return result;
    }

    public static async Task<Dictionary<TKey, TValue>> GetDictionaryAsync<TKey, TValue>(this SqlConnection db, string query, object? parameters = null)
        where TKey : notnull
        where TValue : struct
    {
        var result = new Dictionary<TKey, TValue>();

        using var reader = await GetDataReaderAsync(db, query, parameters);

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

    public static async Task<Dictionary<string, object?>> GetObjectAsDictionaryAsync(this SqlConnection db, string query, object? parameters = null)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        using var reader = await GetDataReaderAsync(db, query, parameters);

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

    public static async Task<int> ExecuteAsync(this SqlConnection db, string query, object? parameters = null)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query, parameters);

        return await command.ExecuteNonQueryAsync();
    }

    public static async Task<TIdentity> ExecuteAndSelectIdentityAsync<TIdentity>(this SqlConnection db, string query, object? parameters = null)
    {
        var value = await GetValueAsync<decimal>(db, query + "; select @@identity;", parameters);

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

    public static async Task<SqlDataReader> GetDataReaderAsync(this SqlConnection db, string query, object? parameters = null)
    {
        await EnsureConnectionNotClosedAsync(db);

        using var command = CreateCommand(db, query, parameters);

        return await command.ExecuteReaderAsync();
    }
}
