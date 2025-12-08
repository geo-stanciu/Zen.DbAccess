using DataAccess.Models;

namespace DataAccess.Repositories
{
    public interface IPeopleRepository
    {
        Task CreateTablesAsync();
        Task DropTablesAsync();
        Task<List<Person>> GetAllAsync();
        Task<Person> GetByIdAsync(int personId);
        Task<int> CreateAsync(Person p);
        Task CreateBatchAsync(List<Person> people);
        Task BulkInsertAsync(List<Person> people);
        Task DeleteAsync(int id);
        Task UpdateAsync(Person p);
    }
}