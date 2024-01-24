using System;
using System.Data.Common;
using System.Text;
using Zen.DbAccess.Models;
using Zen.DbAccess.DatabaseSpeciffic;
using Oracle.ManagedDataAccess.Types;
using Oracle.ManagedDataAccess.Client;

namespace Zen.DbAccess.Oracle;

public class DatabaseSpeciffic : IDbSpeciffic
{
    public void DisposeBlob(DbCommand cmd, SqlParam prm)
    {
        if (prm.value != null && prm.value != DBNull.Value)
        {
            string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

            if (cmd.Parameters[baseParameterName].Value as OracleBlob != null)
                (cmd.Parameters[baseParameterName].Value as OracleBlob)!.Dispose();
        }
    }

    public void DisposeClob(DbCommand cmd, SqlParam prm)
    {
        if (prm.value != null && prm.value != DBNull.Value)
        {
            string baseParameterName = prm.name.StartsWith("@") ? prm.name.Substring(1) : prm.name;

            if (cmd.Parameters[baseParameterName].Value as OracleClob != null)
                (cmd.Parameters[baseParameterName].Value as OracleClob)!.Dispose();
        }
    }

    public object GetValueAsBlob(DbConnection conn, object value)
    {
        OracleBlob blob = new OracleBlob(conn as OracleConnection);
        byte[] byteContent = (value as byte[])!;
        blob.Write(byteContent, 0, byteContent.Length);

        return blob;
    }

    public object GetValueAsClob(DbConnection conn, object value)
    {
        OracleClob clob = new OracleClob(conn as OracleConnection);
        byte[] byteContent = Encoding.Unicode.GetBytes((value as string)!);
        clob.Write(byteContent, 0, byteContent.Length);

        return clob;
    }

    public void PrepareDataAdapter(DbDataAdapter? da)
    {
        if (da != null)
            (da as OracleDataAdapter)!.SuppressGetDecimalInvalidCastException = true;
    }
}
