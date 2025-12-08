using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.MariaDb.Factories;

public static class MariaDbDbConnectionFactory
{
    public static DbConnectionFactory Create(
        string conn_str,
        bool commitNoWait = true,
        string timeZone = "",
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.MARIADB, MySqlConnectorFactory.Instance);

        return new DbConnectionFactory(
            DbConnectionType.MariaDb,
            conn_str,
            new MariaDbDatabaseSpeciffic(),
            commitNoWait,
            timeZone,
            dbNamingConvention);
    }
}
