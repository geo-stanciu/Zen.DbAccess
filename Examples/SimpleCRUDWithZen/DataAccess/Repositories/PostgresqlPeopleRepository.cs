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
using Zen.DbAccess.Repositories;

namespace DataAccess.Repositories;

public class PostgresqlPeopleRepository : PeopleBaseRepository
{
    public PostgresqlPeopleRepository(
        [FromKeyedServices(DataSourceNames.Postgresql)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public override async Task CreateTablesAsync()
    {
        string sql = $"""
            create table if not exists {PERSON_TABLE_NAME} (
                id serial not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                birth_date date,
                type int,
                image bytea,
                created_at timestamp,
                updated_at timestamp,
                constraint person_pk primary key (id)
            )
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            create table if not exists {UPLOADS_TABLE_NAME} (
                id serial not null,
                long_value bigint,
                decimal_value decimal(22,8),
                text_value varchar(512),
                date_value timestamp,
                file_name varchar(256),
                file bytea,
                created_at timestamp not null,
                updated_at timestamp,
                constraint uploads_pk primary key (id)
            )
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            CREATE OR REPLACE FUNCTION {P_GET_ALL_PEOPLE}()
             RETURNS table(
                 id int,
                 first_name varchar(128),
                 last_name varchar(128),
                 type int,
                 birth_date date,
                 image bytea,
                 created_at timestamp,
                 updated_at timestamp,
                 is_error int,
                 error_message varchar(512)
             )
             LANGUAGE plpgsql
             SECURITY DEFINER
            AS $function$
            DECLARE
              lError   int := 0;
              sError   varchar(512) := '';
            begin
               	RETURN QUERY
               	select
            	    s.id
            	    , s.first_name
            	    , s.last_name
            	    , s.type
            	    , s.birth_date
            	    , s.image
            	    , s.created_at
            	    , s.updated_at
                    , lError as is_error
                    , sError as error_message
            	    from svm.person s
            	   order by s.id;
            END
            $function$
            ;
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            CREATE OR REPLACE FUNCTION {P_GET_ALL_PEOPLE_MULTI_RESULT_SET}
             RETURNS SETOF refcursor
             LANGUAGE plpgsql
             SECURITY DEFINER
            AS $function$
            DECLARE
              lError   int := 0;
              sError   varchar(512) := '';
              v_cursor refcursor;
              v_cursor2 refcursor;
            begin
               	OPEN v_cursor FOR
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
            	    from svm.person 
            	   order by id;

               	RETURN NEXT v_cursor;

                OPEN v_cursor2 FOR
               	select 1 as is_error, 'this is a test' as error_message;

               	RETURN NEXT v_cursor2;
            END
            $function$
            ;
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

        sql = $"""
            drop function if exists {P_GET_ALL_PEOPLE};
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);

        sql = $"""
            drop function if exists {P_GET_ALL_PEOPLE_MULTI_RESULT_SET};
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory!);
    }
}
