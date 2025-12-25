using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;

namespace Zen.DbAccess.Attributes;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
public class DbNameAttribute : Attribute
{
    public HashSet<DbConnectionType>? DbTypes { get; set; }
 
    public string? DbColumn { get; set; }

    public DbNameAttribute(string dbColumnName)
    {
        DbColumn = NormalizeDbColumn(dbColumnName);
    }

    public DbNameAttribute(string dbColumnName, params DbConnectionType[] dbTypes)
    {
        DbColumn = NormalizeDbColumn(dbColumnName);
        DbTypes = dbTypes.ToHashSet();
    }

    private string NormalizeDbColumn(string dbColumnName)
    {
        var dbCol = dbColumnName;

        if ((dbCol.StartsWith("\"") || dbCol.StartsWith("[")) && (dbCol.EndsWith("\"") || dbCol.EndsWith("]")))
            dbCol = dbCol.Substring(1, dbCol.Length - 2);
        else if (dbCol.StartsWith("\"") || dbCol.StartsWith("["))
            dbCol = dbCol.Substring(1);
        else if (dbCol.EndsWith("\"") || dbCol.EndsWith("]"))
            dbCol = dbCol.Substring(0, dbCol.Length - 1);

        return dbCol;
    }
}