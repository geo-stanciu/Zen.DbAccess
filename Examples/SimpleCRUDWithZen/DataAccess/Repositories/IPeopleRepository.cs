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
        Task DeleteAsync(int id);
        Task UpdateAsync(Person p);
    }
}