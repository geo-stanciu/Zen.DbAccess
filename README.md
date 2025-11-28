# Zen.DbAccess

Examples for setup basic CRUD operations in Examples/SimpleCRUDWithZen solution.

### Setup
- Define your connection string in the ConnectionStrings section in appsettings.json

```json
{
  "ConnectionStrings": {
    "Postgresql": "Server=server1;Port=5432;Database=database1;User Id=user1;Password=password1;SSL Mode=prefer;Trust Server Certificate=true;Pooling=true;Application Name=Api1;Search Path=schema1,public;Encoding=UTF8;Timezone=UTC;",
    "Oracle": "Data Source=(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = server1) (PORT = 1521))) (CONNECT_DATA = (SERVICE_NAME = db1.server1)));User ID=user1;Password=pass1;Pooling=true;"
  }
}
```

- If for example your connection string is named "Postgresql", create an enum with this value (ex: Examples/SimpleCRUDWithZen/DataAccess/Enum/DataSourceNames.cs ).

```csharp
public enum DataSourceNames
{
    Postgresql = 0,
    Oracle = 1,
}
```

- Create your extensions class (ex: Examples/SimpleCRUDWithZen/DataAccess/Extensions/DatabaseAccessWebApplicationBuilderExtensions.cs)

```csharp
public static void SetupPostgresqlDatabaseAccess(this IHostApplicationBuilder builder)
{
    builder.AddPostgresqlZenDbAccessConnection(DataSourceNames.Postgresql, nameof(DataSourceNames.Postgresql));
}
```

The connection factory / helper will be registered as a scoped keyd service with the key DataSourceNames.Postgresql.

- Inject your db connection helper / factory (ex: Examples/SimpleCRUDWithZen/DataAccess/Repositories/PostgresqlPeopleRepository.cs)

```csharp
public class PostgresqlPeopleRepository : IPeopleRepository
{
    protected readonly IDbConnectionFactory _dbConnectionFactory;

    protected virtual string TABLE_NAME { get; set; } = "person";

    public PostgresqlPeopleRepository(
        [FromKeyedServices(DataSourceNames.Postgresql)] IDbConnectionFactory dbConnectionFactory)
    {
        _dbConnectionFactory = dbConnectionFactory;
    }
}
```
