using DataAccess.Models;
using Zen.DbAccess.Models;

namespace DataAccess.Repositories;

public interface IPeopleRepository
{
    Task CreateTablesAsync();
    
    Task DropTablesAsync();
    
    Task<List<Person>> GetAllByProcedureAsync()
    {
        throw new NotImplementedException();
    }

    Task<(List<Person>, List<ResponseModel>)> GetAllByProcedureMultiResultAsync()
    {
        throw new NotImplementedException();
    }

    Task<List<Person>> GetAllAsync();
    
    Task<Person> GetByIdAsync(int personId);
    
    Task<int> CreateAsync(Person p);
    
    Task CreateBatchAsync(List<Person> people);
    
    Task BulkInsertAsync(List<Person> people);
    
    Task DeleteAsync(int id);
    
    Task UpdateAsync(Person p);
    
    Task SaveFileAsync(UploadFileModel fileUpload);

    Task<List<UploadFileModel>> GetAllUploadsAsync();

    Task<List<UploadFileModel>> GetAllUploadsWithGeneratedColumnsAsync();
}