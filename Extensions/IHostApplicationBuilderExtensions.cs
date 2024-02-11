using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Factories;

namespace Zen.DbAccess.Extensions;

public static class IHostApplicationBuilderExtensions
{
    public static IHostApplicationBuilder AddZenDbAccessConnection(this IHostApplicationBuilder builder)
    {
        if (!builder.Services.Any(x => x.ServiceType == typeof(IDbConnectionFactory)))
        {
            builder.Services.AddSingleton<IDbConnectionFactory, DbConnectionFactory>(provider => new DbConnectionFactory(provider.GetRequiredService<IConfiguration>()));
        }

        return builder;
    }
}
