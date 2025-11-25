using DataAccess.Enum;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;

namespace SimpleCRUDWithZen;

public static class PostgresqlEndpoints
{
    public static void RegisterPostgresqlEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/postgresql").WithTags("Postgresql Examples");

        group.MapPost("/createtables", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.CreateTablesAsync();

            return Results.NoContent();
        });

        group.MapDelete("/droptables", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.DropTablesAsync();

            return Results.NoContent();
        });


        group.MapGet("/people", async ([FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var people = await repo.GetAllAsync();

            return Results.Ok(people);
        });

        group.MapPost("/people", async ([FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            var personId = await repo.CreateAsync(person);
            person.Id = personId;

            return Results.Created($"/people/{personId}", person);
        });

        group.MapGet("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = await repo.GetByIdAsync(id);

            return Results.Ok(person);
        });

        group.MapPut("/people/{id}", async ([FromRoute] int id, [FromBody] CreateOrUpdatePersonModel p, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            var person = p.ToPerson();
            person.Id = id;

            await repo.UpdateAsync(person);

            return Results.NoContent();
        });

        group.MapDelete("/people/{id}", async ([FromRoute] int id, [FromKeyedServices(DataSourceNames.Postgresql)] IPeopleRepository repo) =>
        {
            await repo.DeleteAsync(id);

            return Results.NoContent();
        });
    }
}
