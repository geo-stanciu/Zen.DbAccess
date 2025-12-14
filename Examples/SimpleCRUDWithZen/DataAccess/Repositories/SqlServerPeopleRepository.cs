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

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sTableName", TABLE_NAME));

        if (!(await ProcedureExistsAsync(P_GET_ALL_PEOPLE)))
        {
            sql = $"""
                CREATE PROCEDURE {P_GET_ALL_PEOPLE}
                AS
                begin
                    SET NOCOUNT ON; -- Prevents the count of the number of rows affected from being returned

                    DECLARE @lError int = 0;
                    DECLARE @sError varchar(512) = '';
                    
                    select
                        id
                        , first_name
                        , last_name
                        , type
                        , birth_date
                        , image
                        , created_at
                        , updated_at
                        , @lError as is_error
                        , @sError as error_message
                      from person 
                      order by id;
                END
                """;

            _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sProcName", P_GET_ALL_PEOPLE));
        }
    }

    private async Task<bool> ProcedureExistsAsync(string procName)
    {
        string sql = "SELECT 1 as procedure_exists FROM sys.procedures WHERE name = @sProcName ";

        var exists = await sql.QueryRowAsync<SqlServerProcedureExistsModel>(
            _dbConnectionFactory!,
            new SqlParam("@sProcName", procName)
        ) ?? new SqlServerProcedureExistsModel();

        return exists.ProcedureExists;
    }

    public override async Task DropTablesAsync()
    {
        string sql = $"""
            IF EXISTS (SELECT * FROM sys.tables WHERE name = @sTableName)
            BEGIN
                drop table if exists {TABLE_NAME};
            END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sTableName", TABLE_NAME));

        sql = $"""
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = @sProcName)
            BEGIN
                drop procedure if exists {P_GET_ALL_PEOPLE};
            END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sProcName", P_GET_ALL_PEOPLE));
    }
}
