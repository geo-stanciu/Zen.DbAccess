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

public class SimpleRepository : ISimpleRepository
{
    private readonly IDbConnectionFactory _dbConnectionFactory;

    public SimpleRepository(
        [FromKeyedServices(DataSourceNames.Default)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }

    public async Task CreateTableAsync()
    {
        string sql = $"""
            create table if not exists person (
                id serial not null,
                first_name varchar(128),
                last_name varchar(128) not null,
                constraint person_pk primary key (Id)
            )
            """;

        _ = await sql.ExecuteNonQueryAsync(_dbConnectionFactory);
    }

    public async Task<int> InsertPersonAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, "person");

        return p.Id;
    }

    public async Task UpdatePersonAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory, "person");
    }

    public async Task RemovePersonAsync(int id)
    {
        var p = await GetPersonByIdAsync(id);

        await p.DeleteAsync(_dbConnectionFactory, "person");
    }

    public async Task<List<Person>> GetAllPeopleAsync()
    {
        string sql = $"""
            select id, first_name, last_name from person order by id
            """;

        var people = await sql.QueryAsync<Person>(_dbConnectionFactory);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return people;
    }

    public async Task<Person> GetPersonByIdAsync(int personId)
    {
        string sql = $"""
            select id, first_name, last_name from person where id = @Id
            """;

        var p = await sql.QueryRowAsync<Person>(_dbConnectionFactory, new SqlParam("@Id", personId));

        if (p == null)
            throw new NullReferenceException(nameof(p));

        return p;
    }
}
