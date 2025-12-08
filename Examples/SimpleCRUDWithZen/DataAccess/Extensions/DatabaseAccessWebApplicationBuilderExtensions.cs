using DataAccess.Enum;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.MariaDb.Extensions;
using Zen.DbAccess.Oracle.Extensions;
using Zen.DbAccess.Postgresql.Extensions;
using Zen.DbAccess.Sqlite.Extensions;
using Zen.DbAccess.SqlServer.Extensions;

namespace DataAccess.Extensions;

public static class DatabaseAccessWebApplicationBuilderExtensions
{
    public static void SetupPostgresqlDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddPostgresqlZenDbAccessConnection(DataSourceNames.Postgresql, nameof(DataSourceNames.Postgresql));
    }

    public static void SetupOracleDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddOracleZenDbAccessConnection(DataSourceNames.Oracle, nameof(DataSourceNames.Oracle));
    }

    public static void SetupMariaDbDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddMariaDbZenDbAccessConnection(DataSourceNames.MariaDb, nameof(DataSourceNames.MariaDb));
    }

    public static void SetupSqlServerDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddSqlServerZenDbAccessConnection(DataSourceNames.SqlServer, nameof(DataSourceNames.SqlServer));
    }

    public static void SetupSqliteDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddSqliteZenDbAccessConnection(DataSourceNames.Sqlite, nameof(DataSourceNames.Sqlite));
    }
}
