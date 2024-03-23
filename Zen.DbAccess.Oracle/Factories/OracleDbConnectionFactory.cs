using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.Oracle.Factories;

public static class OracleDbConnectionFactory
{
    public static DbConnectionFactory Create(string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.ORACLE, OracleClientFactory.Instance);

        return new DbConnectionFactory(
            DbConnectionType.Oracle,
            conn_str,
            new OracleDatabaseSpeciffic(),
            commitNoWait,
            timeZone);
    }
}
