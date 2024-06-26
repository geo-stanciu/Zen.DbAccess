﻿using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Factories;

public interface IDbConnectionFactory
{
    Task<IZenDbConnection> BuildAsync();
    DbConnectionType DbType { get; set; }
    string? ConnectionString { get; set; }
    IDbConnectionFactory Copy(string? newConnectionString = null);
}