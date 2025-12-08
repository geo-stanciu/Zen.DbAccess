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

public class MariaDbPeopleRepository : PostgresqlPeopleRepository, IPeopleRepository
{
    public MariaDbPeopleRepository(
        [FromKeyedServices(DataSourceNames.MariaDb)] IDbConnectionFactory dbConnectionFactory)
        : base (dbConnectionFactory)
    {
    }
    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            create table if not exists {TABLE_NAME} (
                id int auto_increment not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                birth_date date,
                type int,
                image longblob,
                created_at datetime(6),
                updated_at datetime(6),
                constraint person_pk primary key (id)
            ) character set utf8mb4 collate utf8mb4_unicode_ci
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public override async Task DropTablesAsync()
    {
        string sql = $"""
            drop table if exists {TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }
}
