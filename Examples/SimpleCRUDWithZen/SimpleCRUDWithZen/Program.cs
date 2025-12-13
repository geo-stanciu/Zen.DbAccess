using DataAccess.Enum;
using DataAccess.Extensions;
using DataAccess.Repositories;
using Microsoft.OpenApi;
using SimpleCRUDWithZen;
using Swashbuckle.AspNetCore.SwaggerUI;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "Zen.DbAccess Examples V1", Version = "v1" });
});

builder.Services.AddAntiforgery(o => o.HeaderName = "X-CSRF-TOKEN");

builder.SetupPostgresqlDatabaseAccess();
builder.SetupOracleDatabaseAccess();
builder.SetupMariaDbDatabaseAccess();
builder.SetupSqlServerDatabaseAccess();
builder.SetupSqliteDatabaseAccess();

builder.Services.AddKeyedScoped<IPeopleRepository, PostgresqlPeopleRepository>(DataSourceNames.Postgresql);
builder.Services.AddKeyedScoped<IPeopleRepository, OraclePeopleRepository>(DataSourceNames.Oracle);
builder.Services.AddKeyedScoped<IPeopleRepository, MariaDbPeopleRepository>(DataSourceNames.MariaDb);
builder.Services.AddKeyedScoped<IPeopleRepository, SqlServerPeopleRepository>(DataSourceNames.SqlServer);
builder.Services.AddKeyedScoped<IPeopleRepository, SqlitePeopleRepository>(DataSourceNames.Sqlite);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "Zen.DbAccess Examples V1");
        c.DocExpansion(DocExpansion.None);
    });
}

app.UseAntiforgery();

app.UseHttpsRedirection();

app.RegisterPostgresqlEndpoints();
app.RegisterOracleEndpoints();
app.RegisterMariaDbEndpoints();
app.RegisterSqlServerEndpoints();
app.RegisterSqliteEndpoints();

app.Run();
