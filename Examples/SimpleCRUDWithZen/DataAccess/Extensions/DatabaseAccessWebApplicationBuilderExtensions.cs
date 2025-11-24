using DataAccess.Enum;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Oracle.Extensions;
using Zen.DbAccess.Postgresql.Extensions;

namespace DataAccess.Extensions;

public static class DatabaseAccessWebApplicationBuilderExtensions
{
    public static void SetupDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddPostgresqlZenDbAccessConnection(DataSourceNames.Default, nameof(DataSourceNames.Default));
    }

    public static void SetupOracleDatabaseAccess(this IHostApplicationBuilder builder)
    {
        // setup zen db access

        builder
            .AddOracleZenDbAccessConnection(DataSourceNames.Oracle, nameof(DataSourceNames.Oracle));
    }
}
