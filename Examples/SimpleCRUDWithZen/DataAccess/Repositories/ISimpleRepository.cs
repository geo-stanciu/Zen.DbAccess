using DataAccess.Models;

namespace DataAccess.Repositories
{
    public interface ISimpleRepository
    {
        Task CreateTableAsync();
        Task<List<Person>> GetAllPeopleAsync();
        Task<Person> GetPersonByIdAsync(int personId);
        Task<int> InsertPersonAsync(Person p);
        Task RemovePersonAsync(int id);
        Task UpdatePersonAsync(Person p);
    }
}