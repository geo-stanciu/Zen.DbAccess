using DataAccess.Enum;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Postgresql.Extensions;

namespace DataAccess.Extensions;

public static class DatabaseAccessWebApplicationBuilderExtensions
{
    public static void SetupDatabaseAccess(this IHostApplicationBuilder builder)
    {
        IConfigurationSection appSettingsSection = builder.Configuration.GetSection("AppSettings");

        // setup zen db access

        builder
            .AddPostgresqlZenDbAccessConnection(DataSourceNames.Default, nameof(DataSourceNames.Default));
    }
}
