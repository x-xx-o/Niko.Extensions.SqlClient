namespace Microsoft.Data.SqlClient;

public readonly struct RawSqlString
{
    public string Value { get; }

    public RawSqlString(string value) => Value = value;
    public static implicit operator RawSqlString(string value) => new(value);
    public static implicit operator RawSqlString(FormattableString value) => new(value.Format);
}