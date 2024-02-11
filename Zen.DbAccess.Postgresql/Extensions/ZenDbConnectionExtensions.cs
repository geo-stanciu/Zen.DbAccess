using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Postgresql.Extensions;

public static class ZenDbConnectionExtensions
{
    public static Task<DataTable> ExecuteCursorToTableAsync(this IZenDbConnection conn, string cursorName)
    {
        string sql = $"FETCH ALL IN \"{cursorName}\"";

        using DbCommand cmd = conn.Connection.CreateCommand();

        if (conn.Transaction != null && cmd.Transaction == null)
            cmd.Transaction = conn.Transaction;

        cmd.CommandText = sql;

        using DbDataAdapter da = conn.DatabaseSpeciffic.CreateDataAdapter(conn);
        da.SelectCommand = cmd;

        DataTable dt = new DataTable();
        da.Fill(dt);

        return Task.FromResult(dt);
    }
}
