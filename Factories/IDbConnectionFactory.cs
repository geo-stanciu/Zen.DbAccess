using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.Factories;

public interface IDbConnectionFactory
{
    Task<IZenDbConnection> BuildAsync();
    DbConnectionType DbType { get; set; }
    string? ConnectionString { get; set; }
    DbNamingConvention DbNamingConvention { get; set; }
    IDbSpeciffic DatabaseSpeciffic { get; set; }
    string GenerateQueryColumns<T>() where T: DbModel;
    IDbConnectionFactory Copy(string? newConnectionString = null);
}