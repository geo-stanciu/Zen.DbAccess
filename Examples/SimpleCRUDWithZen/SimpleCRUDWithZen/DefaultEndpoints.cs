using DataAccess.Enum;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace SimpleCRUDWithZen;

public static class DefaultEndpoints
{
    public static void RegisterPostgresqlEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/createtables", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.CreateTablesAsync();

            return Results.NoContent();
        })
        .WithName("CreateTables");

        app.MapDelete("/droptables", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.DropTablesAsync();

            return Results.NoContent();
        })
        .WithName("DropTables");


        app.MapGet("/people", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var people = await repo.GetAllAsync();

            return Results.Ok(people);
        })
        .WithName("GetPeople");

        app.MapPost("/people", async ([FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            var personId = await repo.CreateAsync(person);
            person.Id = personId;

            return Results.Created($"/people/{personId}", person);
        })
        .WithName("CreatePerson");

        app.MapGet("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = await repo.GetByIdAsync(id);

            return Results.Ok(person);
        })
        .WithName("GetPerson");

        app.MapPut("/people/{id}", async ([FromRoute] int id, [FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            person.Id = id;

            await repo.UpdateAsync(person);

            return Results.NoContent();
        })
        .WithName("UpdatePerson");

        app.MapDelete("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.DeleteAsync(id);

            return Results.NoContent();
        })
        .WithName("DeletePerson");
    }
}
