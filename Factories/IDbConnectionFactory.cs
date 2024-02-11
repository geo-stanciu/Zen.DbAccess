using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Factories
{
    public interface IDbConnectionFactory
    {
        Task<IZenDbConnection> BuildAsync();
        DbConnectionType DbType { get; }

        IDbConnectionFactory Get(string connectionStringName);
        IDbConnectionFactory Create(DbConnectionType dbType, string conn_str, bool commitNoWait = true, string timeZone = "");
        void RegisterConnection(DbConnectionType dbType, string connectionStringName);
    }
}