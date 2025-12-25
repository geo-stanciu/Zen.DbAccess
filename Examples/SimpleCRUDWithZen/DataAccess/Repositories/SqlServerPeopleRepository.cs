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

public class SqlServerPeopleRepository : PeopleBaseRepository
{
    public SqlServerPeopleRepository(
        [FromKeyedServices(DataSourceNames.SqlServer)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }
    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @sTableName)
            BEGIN
                create table {PERSON_TABLE_NAME} (
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

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sTableName", PERSON_TABLE_NAME));

        sql = $"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = @sTableName)
            BEGIN
                create table {UPLOADS_TABLE_NAME} (
                    id int identity(1,1) not null,
                    long_value bigint,
                    decimal_value decimal(22,8),
                    text_value nvarchar(512),
                    date_value datetime2(6),
                    file_name varchar(256),
                    "FILE" varbinary(max),
                    created_at datetime2(6) not null,
                    updated_at datetime2(6),
                    constraint uploads_pk primary key (id)
                );
                END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sTableName", UPLOADS_TABLE_NAME));

        if (!await ProcedureExistsAsync(P_GET_ALL_PEOPLE))
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

        if (!await ProcedureExistsAsync(P_GET_ALL_PEOPLE_MULTI_RESULT_SET))
        {
            sql = $"""
                CREATE PROCEDURE {P_GET_ALL_PEOPLE_MULTI_RESULT_SET}
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

                    select 1 as is_error, 'this is a test' as error_message;
                END
                """;

            _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sProcName", P_GET_ALL_PEOPLE_MULTI_RESULT_SET));
        }
    }

    public override async Task<List<UploadFileModel>> GetAllUploadsAsync()
    {
        string sql = $"""
            select id
                   , long_value
                   , decimal_value
                   , text_value
                   , date_value
                   , file_name
                   , [FILE]
                   , created_at
                   , updated_at
              from {UPLOADS_TABLE_NAME}
             order by id
            """;

        var uploads = await sql.QueryAsync<UploadFileModel>(_dbConnectionFactory!);

        if (uploads == null)
            throw new NullReferenceException(nameof(uploads));

        return uploads;
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
                drop table if exists {PERSON_TABLE_NAME};
            END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sTableName", PERSON_TABLE_NAME));

        sql = $"""
            IF EXISTS (SELECT * FROM sys.procedures WHERE name = @sProcName)
            BEGIN
                drop procedure if exists {P_GET_ALL_PEOPLE_MULTI_RESULT_SET};
            END;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!, new SqlParam("@sProcName", P_GET_ALL_PEOPLE_MULTI_RESULT_SET));
    }
}
