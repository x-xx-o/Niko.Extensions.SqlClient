namespace Microsoft.Data.SqlClient;

class SqlQuery
{
    public string QueryString { get; set; } = "";
    public List<SqlParameter> Parameters { get; set; } = new();
}