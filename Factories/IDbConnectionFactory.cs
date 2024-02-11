using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Factories
{
    public interface IDbConnectionFactory
    {
        Task<IZenDbConnection> BuildAsync();
        IDbConnectionFactory Create(string connectionStringName);
    }
}