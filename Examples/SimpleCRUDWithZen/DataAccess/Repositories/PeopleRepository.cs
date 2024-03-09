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

public class PeopleRepository : IPeopleRepository
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    private const string TABLE_NAME = "person";

    public PeopleRepository(
        [FromKeyedServices(DataSourceNames.Default)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task CreateTablesAsync()
    {
        string sql = $"""
            create table if not exists {TABLE_NAME} (
                id serial not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                constraint person_pk primary key (Id)
            )
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public async Task DropTablesAsync()
    {
        string sql = $"""
            drop table if exists {TABLE_NAME}
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public async Task<int> CreateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, TABLE_NAME);

        return p.Id;
    }

    public async Task UpdateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, TABLE_NAME);
    }

    public async Task DeleteAsync(int id)
    {
        var p = await GetByIdAsync(id);

        await p.DeleteAsync(_dbConnectionFactory, TABLE_NAME);
    }

    public async Task<List<Person>> GetAllAsync()
    {
        string sql = $"""
            select id, first_name, last_name from {TABLE_NAME} order by id
            """;

        var people = await sql.QueryAsync<Person>(_dbConnectionFactory);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return people;
    }

    public async Task<Person> GetByIdAsync(int personId)
    {
        string sql = $"""
            select id, first_name, last_name from {TABLE_NAME} where id = @Id
            """;

        var p = await sql.QueryRowAsync<Person>(_dbConnectionFactory, new SqlParam("@Id", personId));

        if (p == null)
            throw new NullReferenceException(nameof(p));

        return p;
    }
}
