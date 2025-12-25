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

public class SqlitePeopleRepository : PeopleBaseRepository
{
    public SqlitePeopleRepository(
        [FromKeyedServices(DataSourceNames.Sqlite)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }
    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            PRAGMA journal_mode=WAL;

            create table if not exists {PERSON_TABLE_NAME} (
                id integer primary key not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                birth_date date,
                type integer,
                image blob,
                created_at timestamp,
                updated_at timestamp
            );

            create table if not exists {UPLOADS_TABLE_NAME} (
                id integer primary key not null,
                long_value integer,
                decimal_value real,
                text_value varchar(512),
                date_value timestamp,
                file_name varchar(256),
                file blob,
                created_at timestamp not null,
                updated_at timestamp
            );
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);
    }

    public override async Task DropTablesAsync()
    {
        string sql = $"""
            drop table if exists {PERSON_TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            drop table if exists {UPLOADS_TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

    }
}
