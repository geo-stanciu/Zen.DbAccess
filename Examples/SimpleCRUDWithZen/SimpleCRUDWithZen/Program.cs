using DataAccess.Enum;
using DataAccess.Extensions;
using DataAccess.Models;
using DataAccess.Repositories;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using SimpleCRUDWithZen;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.SetupDatabaseAccess();
builder.SetupOracleDatabaseAccess();

builder.Services.AddKeyedScoped<IPeopleRepository, PeopleRepository>(DataSourceNames.Default);
builder.Services.AddKeyedScoped<IPeopleRepository, OraclePeopleRepository>(DataSourceNames.Oracle);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.RegisterDefaultEndpoints();
app.RegisterOracleEndpoints();

app.Run();
