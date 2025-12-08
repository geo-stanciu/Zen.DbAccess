using DataAccess.Enum;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace SimpleCRUDWithZen;

public static class OracleEndpoints
{
    private const DataSourceNames dataSource = DataSourceNames.Oracle;

    public static void RegisterOracleEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/oracle").WithTags("Oracle Examples");

        group.MapPost("/createtables", async ([FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            await repo.CreateTablesAsync();

            return Results.NoContent();
        });

        group.MapDelete("/droptables", async ([FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            await repo.DropTablesAsync();

            return Results.NoContent();
        });

        group.MapGet("/people", async ([FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var people = await repo.GetAllAsync();

            return Results.Ok(people);
        });

        group.MapGet("/people/ByProcedure", async ([FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var people = await repo.GetAllByProcedureAsync();

            return Results.Ok(people);
        });

        group.MapPost("/people", async ([FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            person.CreatedAt = DateTime.UtcNow;

            var personId = await repo.CreateAsync(person);
            person.Id = personId;

            return Results.Created($"/people/{personId}", person);
        });

        group.MapPost("/people/batch", async ([FromBody] List<CreateOrUpdatePersonModel> p, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var utcNow = DateTime.UtcNow;

            var people = p.Select(x => x.ToPerson()).ToList();
            people.ForEach(x => x.CreatedAt = utcNow);

            await repo.CreateBatchAsync(people);

            return Results.Created($"/people", people);
        });

        group.MapPost("/people/bulkinsert", async ([FromBody] List<CreateOrUpdatePersonModel> p, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var utcNow = DateTime.UtcNow;

            var people = p.Select(x => x.ToPerson()).ToList();
            people.ForEach(x => x.CreatedAt = utcNow);

            await repo.BulkInsertAsync(people);

            return Results.Created($"/people", people);
        });

        group.MapGet("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var person = await repo.GetByIdAsync(id);

            return Results.Ok(person);
        });

        group.MapPut("/people/{id}", async ([FromRoute] int id, [FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            var person = await repo.GetByIdAsync(id);
            person.FirstName = p.FirstName;
            person.LastName = p.LastName;
            person.BirthDate = p.BirthDate;
            person.Type = p.Type;
            person.Image = p.Image;
            person.UpdatedAt = DateTime.UtcNow;

            await repo.UpdateAsync(person);

            return Results.NoContent();
        });

        group.MapDelete("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(dataSource)] IPeopleRepository repo) =>
        {
            await repo.DeleteAsync(id);

            return Results.NoContent();
        });
    }
}
