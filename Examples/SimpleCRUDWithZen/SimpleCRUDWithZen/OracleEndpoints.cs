using DataAccess.Enum;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace SimpleCRUDWithZen;

public static class OracleEndpoints
{
    public static void RegisterOracleEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/oracle_createtables", async ([FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            await repo.CreateTablesAsync();

            return Results.NoContent();
        })
        .WithName("oracle_CreateTables");

        app.MapDelete("/oracle_droptables", async ([FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            await repo.DropTablesAsync();

            return Results.NoContent();
        })
        .WithName("oracle_DropTables");


        app.MapGet("/oracle_people", async ([FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            var people = await repo.GetAllAsync();

            return Results.Ok(people);
        })
        .WithName("oracle_GetPeople");

        app.MapPost("/oracle_people", async ([FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            var personId = await repo.CreateAsync(person);
            person.Id = personId;

            return Results.Created($"/oracle_people/{personId}", person);
        })
        .WithName("oracle_CreatePerson");

        app.MapGet("/oracle_people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            var person = await repo.GetByIdAsync(id);

            return Results.Ok(person);
        })
        .WithName("oracle_GetPerson");

        app.MapPut("/oracle_people/{id}", async ([FromRoute] int id, [FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            person.Id = id;

            await repo.UpdateAsync(person);

            return Results.NoContent();
        })
        .WithName("oracle_UpdatePerson");

        app.MapDelete("/oracle_people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Oracle)] IPeopleRepository repo) =>
        {
            await repo.DeleteAsync(id);

            return Results.NoContent();
        })
        .WithName("oracle_DeletePerson");
    }
}
