using DataAccess.Models;
using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Models;
using Zen.DbAccess.Repositories;

namespace DataAccess.Repositories;

public class PeopleBaseRepository : BaseRepository, IPeopleRepository
{
    protected virtual string PERSON_TABLE_NAME { get; set; } = "person";
    
    protected virtual string UPLOADS_TABLE_NAME { get; set; } = "uploads";

    protected virtual string P_GET_ALL_PEOPLE { get; set; } = "p_get_all_people";

    protected virtual string P_GET_ALL_PEOPLE_MULTI_RESULT_SET { get; set; } = "p_get_all_people_multi_result_set";

    public virtual async Task CreateTablesAsync()
    {
        throw new NotImplementedException();
    }

    public virtual async Task DropTablesAsync()
    {
        throw new NotImplementedException();
    }

    public async Task<List<Person>> GetAllByProcedureAsync()
    {
        string sql = P_GET_ALL_PEOPLE;

        var people = await RunTableProcedureAsync<Person>(sql);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return people;
    }

    public async Task<(List<Person>, List<ResponseModel>)> GetAllByProcedureMultiResultAsync()
    {
        string sql = P_GET_ALL_PEOPLE_MULTI_RESULT_SET;

        var (people, errors) = await RunProcedureAsync<Person, ResponseModel>(sql);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return (people, errors);
    }

    public virtual async Task<int> CreateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory!, PERSON_TABLE_NAME);

        return p.Id;
    }

    public virtual async Task CreateBatchAsync(List<Person> people)
    {
        await people.SaveAllAsync(_dbConnectionFactory!, PERSON_TABLE_NAME);
    }

    public virtual async Task BulkInsertAsync(List<Person> people)
    {
        await using var conn = await _dbConnectionFactory!.BuildAsync();

        await people.BulkInsertAsync(conn, PERSON_TABLE_NAME);
    }

    public virtual async Task UpdateAsync(Person p)
    {
        await p.SaveAsync(_dbConnectionFactory!, PERSON_TABLE_NAME);
    }

    public virtual async Task DeleteAsync(int id)
    {
        var p = new Person { Id = id };

        await p.DeleteAsync(_dbConnectionFactory!, PERSON_TABLE_NAME);
    }

    public virtual async Task<List<Person>> GetAllAsync()
    {
        string sql = $"""
            select id, first_name, last_name, type, birth_date, image, created_at, updated_at from {PERSON_TABLE_NAME} order by id
            """;

        var people = await sql.QueryAsync<Person>(_dbConnectionFactory!);

        if (people == null)
            throw new NullReferenceException(nameof(people));

        return people;
    }

    public virtual async Task<List<UploadFileModel>> GetAllUploadsAsync()
    {
        string sql = $"""
            select id
                   , long_value
                   , decimal_value
                   , text_value
                   , date_value
                   , file_name
                   , file
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

    public virtual async Task<List<UploadFileModel>> GetAllUploadsWithGeneratedColumnsAsync()
    {
        string sql = $"""
            select {_dbConnectionFactory!.GenerateQueryColumns<UploadFileModel>()}
              from {UPLOADS_TABLE_NAME}
             order by id
            """;
        
        var uploads = await sql.QueryAsync<UploadFileModel>(_dbConnectionFactory!);

        if (uploads == null)
            throw new NullReferenceException(nameof(uploads));

        return uploads;
    }

    public virtual async Task<Person> GetByIdAsync(int personId)
    {
        string sql = $"""
            select id, first_name, last_name, type, birth_date, image, created_at, updated_at from {PERSON_TABLE_NAME} where id = @Id
            """;

        var p = await sql.QueryRowAsync<Person>(_dbConnectionFactory!, new SqlParam("@Id", personId));

        if (p == null)
            throw new NullReferenceException(nameof(p));

        return p;
    }

    public virtual async Task SaveFileAsync(UploadFileModel fileUpload)
    {
        await fileUpload.SaveAsync(_dbConnectionFactory!, UPLOADS_TABLE_NAME);
    }
}
