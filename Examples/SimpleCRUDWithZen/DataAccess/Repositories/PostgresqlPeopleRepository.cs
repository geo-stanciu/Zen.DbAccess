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

public class PostgresqlPeopleRepository : IPeopleRepository
{
    protected readonly IDbConnectionFactory _dbConnectionFactory;

    protected virtual string TABLE_NAME { get; set; } = "person";

    public PostgresqlPeopleRepository(
        [FromKeyedServices(DataSourceNames.Postgresql)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public virtual async Task CreateTablesAsync()
    {
        string sql = $"""
            create table if not exists {TABLE_NAME} (
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

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public virtual async Task DropTablesAsync()
    {
        string sql = $"""
            drop table if exists {TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public virtual async Task<int> CreateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, TABLE_NAME);

        return p.Id;
    }

    public virtual async Task UpdateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, TABLE_NAME);
    }

    public virtual async Task DeleteAsync(int id)
    {
        var p = new Person { Id = id };

        await p.DeleteAsync(_dbConnectionFactory, TABLE_NAME);
    }

    public virtual async Task<List<Person>> GetAllAsync()
    {
        string sql = $"""
            select id, first_name, last_name, type, birth_date, image, created_at, updated_at from {TABLE_NAME} order by id
            """;

        var people = await sql.QueryAsync<Person>(_dbConnectionFactory);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return people;
    }

    public virtual async Task<Person> GetByIdAsync(int personId)
    {
        string sql = $"""
            select id, first_name, last_name, type, birth_date, image, created_at, updated_at from {TABLE_NAME} where id = @Id
            """;

        var p = await sql.QueryRowAsync<Person>(_dbConnectionFactory, new SqlParam("@Id", personId));

        if (p == null)
            throw new NullReferenceException(nameof(p));

        return p;
    }
}
