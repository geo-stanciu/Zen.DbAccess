using DataAccess.Enum;
using DataAccess.Models;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Models;

namespace DataAccess.Repositories;

public class SqlitePeopleRepository : PostgresqlPeopleRepository, IPeopleRepository
{
    public SqlitePeopleRepository(
        [FromKeyedServices(DataSourceNames.Sqlite)] IDbConnectionFactory dbConnectionFactory)
        : base (dbConnectionFactory)
    {
    }
    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            PRAGMA journal_mode=WAL;

            create table if not exists {TABLE_NAME} (
                id integer primary key not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                birth_date date,
                type integer,
                image blob,
                created_at timestamp,
                updated_at timestamp
            );
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);
    }

    public override async Task DropTablesAsync()
    {
        string sql = $"""
            drop table if exists {TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);
    }
}
