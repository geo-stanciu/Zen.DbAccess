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

public class MariaDbPeopleRepository : PeopleBaseRepository
{
    public MariaDbPeopleRepository(
        [FromKeyedServices(DataSourceNames.MariaDb)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            create table if not exists {PERSON_TABLE_NAME} (
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

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            create table if not exists {UPLOADS_TABLE_NAME} (
                id int auto_increment not null,
                long_value bigint,
                decimal_value decimal(22,8),
                text_value varchar(512),
                date_value datetime(6),
                file_name varchar(256),
                file blob,
                created_at datetime(6) not null,
                updated_at datetime(6),
                constraint uploads_pk primary key (id)
            ) character set utf8mb4 collate utf8mb4_unicode_ci
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            CREATE OR REPLACE PROCEDURE {P_GET_ALL_PEOPLE}()
            begin
            	  DECLARE lError int default 0;
                DECLARE sError varchar(512) default '';

               	select
            	    id
            	    , first_name
            	    , last_name
            	    , type
            	    , birth_date
            	    , image
            	    , created_at
            	    , updated_at
                    , lError as is_error
                    , sError as error_message
            	    from person 
            	   order by id;
            END
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            CREATE OR REPLACE PROCEDURE {P_GET_ALL_PEOPLE_MULTI_RESULT_SET}()
            begin
            	  DECLARE lError int default 0;
                DECLARE sError varchar(512) default '';

               	select
            	    id
            	    , first_name
            	    , last_name
            	    , type
            	    , birth_date
            	    , image
            	    , created_at
            	    , updated_at
                    , lError as is_error
                    , sError as error_message
            	    from person 
            	   order by id;

                select 1 as is_error, 'this is a test' as error_message;
            END
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);
    }
}
