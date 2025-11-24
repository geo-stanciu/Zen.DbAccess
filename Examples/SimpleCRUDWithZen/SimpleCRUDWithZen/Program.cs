using DataAccess.Extensions;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.SetupDatabaseAccess();
builder.Services.AddScoped<IPeopleRepository, PeopleRepository>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/createtables", async (IPeopleRepository repo) =>
{
    await repo.CreateTablesAsync();

    return Results.NoContent();
})
.WithName("CreateTables");

app.MapDelete("/droptables", async (IPeopleRepository repo) =>
{
    await repo.DropTablesAsync();

    return Results.NoContent();
})
.WithName("DropTables");


app.MapGet("/people", async (IPeopleRepository repo) =>
{
    var people = await repo.GetAllAsync();

    return Results.Ok(people);
})
.WithName("GetPeople");

app.MapPost("/people", async ([FromBody] Person p, IPeopleRepository repo) =>
{
    var personId = await repo.CreateAsync(p);
    p.Id = personId;

    return Results.Created($"/people/{personId}", p);
})
.WithName("CreatePerson");

app.MapGet("/people/{id}", async ([FromRoute] int id, IPeopleRepository repo) =>
{
    var person = await repo.GetByIdAsync(id);

    return Results.Ok(person);
})
.WithName("GetPerson");

app.MapPut("/people/{id}", async ([FromRoute] int id, [FromBody] Person p, IPeopleRepository repo) =>
{
    p.Id = id;

    await repo.UpdateAsync(p);

    return Results.NoContent();
})
.WithName("UpdatePerson");

app.MapDelete("/people/{id}", async ([FromRoute] int id, IPeopleRepository repo) =>
{
    await repo.DeleteAsync(id);

    return Results.NoContent();
})
.WithName("DeletePerson");

app.Run();
