using Microsoft.Extensions.Configuration;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.Postgresql.Factories;

public static class PostgresqlDbConnectionFactory
{
    public static DbConnectionFactory Create(string conn_str, bool commitNoWait = true, string timeZone = "")
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.POSTGRESQL, NpgsqlFactory.Instance);

        return new DbConnectionFactory(
            DbConnectionType.Postgresql,
            conn_str,
            new PostgresqlDatabaseSpeciffic(),
            commitNoWait,
            timeZone);
    }
}
