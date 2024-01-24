using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Tests.Zen.DbAccess
{
    public abstract class CommonTestSetup
    {
        protected IConfiguration? _config;

        public CommonTestSetup()
        {
            IServiceCollection services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(Configuration);
        }

        protected IConfiguration Configuration
        {
            get
            {
                if (_config == null)
                {
                    var builder = new ConfigurationBuilder()
                        .AddJsonFile($"appsettings.Test.json", optional: true)
                        .AddJsonFile($"secrets.json", optional: true)
                        .AddUserSecrets(Assembly.GetExecutingAssembly(), optional: true, reloadOnChange: true);

                    _config = builder.Build();
                }

                return _config;
            }
        }
    }
}
