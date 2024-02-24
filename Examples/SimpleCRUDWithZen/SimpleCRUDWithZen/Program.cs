using DataAccess.Extensions;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.SetupDatabaseAccess();
builder.Services.AddScoped<ISimpleRepository, SimpleRepository>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapPost("/createtables", async (ISimpleRepository repo) =>
{
    await repo.CreateTableAsync();

    return Results.NoContent();
})
.WithName("CreateTables")
.WithOpenApi();


app.MapGet("/people", async (ISimpleRepository repo) =>
{
    var people = await repo.GetAllPeopleAsync();

    return Results.Ok(people);
})
.WithName("GetPeople")
.WithOpenApi();

app.MapPost("/people", async ([FromBody] Person p, ISimpleRepository repo) =>
{
    var personId = await repo.InsertPersonAsync(p);
    p.Id = personId;

    return Results.Created($"/people/{personId}", p);
})
.WithName("CreatePerson")
.WithOpenApi();

app.MapGet("/people/{id}", async ([FromRoute] int id, ISimpleRepository repo) =>
{
    var person = await repo.GetPersonByIdAsync(id);

    return Results.Ok(person);
})
.WithName("GetPerson")
.WithOpenApi();

app.MapPut("/people/{id}", async ([FromRoute] int id, [FromBody] Person p, ISimpleRepository repo) =>
{
    p.Id = id;

    await repo.UpdatePersonAsync(p);

    return Results.NoContent();
})
.WithName("UpdatePerson")
.WithOpenApi();

app.MapDelete("/people/{id}", async ([FromRoute] int id, ISimpleRepository repo) =>
{
    await repo.RemovePersonAsync(id);

    return Results.NoContent();
})
.WithName("DeletePerson")
.WithOpenApi();

app.Run();
