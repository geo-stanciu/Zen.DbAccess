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

public class SqlServerPeopleRepository : PostgresqlPeopleRepository, IPeopleRepository
{
    public SqlServerPeopleRepository(
        [FromKeyedServices(DataSourceNames.SqlServer)] IDbConnectionFactory dbConnectionFactory)
        : base (dbConnectionFactory)
    {
    }
    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @sTableName)
            BEGIN
                create table {TABLE_NAME} (
                    id int identity(1,1) not null,
                    first_name nvarchar(128),
                    last_name nvarchar(128) not null,
                    birth_date date,
                    type int,
                    image varbinary(max),
                    created_at datetime2(6),
                    updated_at datetime2(6),
                    constraint person_pk primary key (id)
                );
                END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory, new SqlParam("@sTableName", TABLE_NAME));
    }

    public override async Task DropTablesAsync()
    {
        string sql = $"""
            IF EXISTS (SELECT * FROM sys.tables WHERE name = @sTableName)
            BEGIN
                drop table if exists {TABLE_NAME};
            END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory, new SqlParam("@sTableName", TABLE_NAME));
    }
}
