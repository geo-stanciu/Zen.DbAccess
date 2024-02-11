using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.Oracle.Extensions;

public static class OracleIHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddOracleZenDbAccessConnection(
        this IHostApplicationBuilder builder,
        string connectionName,
        string connectionStringName)
    {
        DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.ORACLE, OracleClientFactory.Instance, new OracleDatabaseSpeciffic());

        DbConnectionFactory.RegisterConnection(connectionName, connectionStringName);

        return builder;
    }
}
