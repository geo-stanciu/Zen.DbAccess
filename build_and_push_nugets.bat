dotnet build -c Release ./Zen.DbAccess.csproj
dotnet pack -c Release ./Zen.DbAccess.csproj

dotnet build -c Release ./Zen.DbAccess.Oracle/Zen.DbAccess.Oracle.csproj
dotnet pack -c Release ./Zen.DbAccess.Oracle/Zen.DbAccess.Oracle.csproj

dotnet build -c Release ./Zen.DbAccess.Postgresql/Zen.DbAccess.Postgresql.csproj
dotnet pack -c Release ./Zen.DbAccess.Postgresql/Zen.DbAccess.Postgresql.csproj

dotnet build -c Release ./Zen.DbAccess.MariaDb/Zen.DbAccess.MariaDb.csproj
dotnet pack -c Release ./Zen.DbAccess.MariaDb/Zen.DbAccess.MariaDb.csproj

dotnet build -c Release ./Zen.DbAccess.Sqlite/Zen.DbAccess.Sqlite.csproj
dotnet pack -c Release ./Zen.DbAccess.Sqlite/Zen.DbAccess.Sqlite.csproj

dotnet build -c Release ./Zen.DbAccess.SqlServer/Zen.DbAccess.SqlServer.csproj
dotnet pack -c Release ./Zen.DbAccess.SqlServer/Zen.DbAccess.SqlServer.csproj

set "PYTHON_EXE=python"

set "PYTHON_SCRIPT=read_proj_version.py"

set "PROJ_FILE=./Zen.DbAccess.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./bin/Release/Zen.DbAccess.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json

set "PROJ_FILE=./Zen.DbAccess.Oracle/Zen.DbAccess.Oracle.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./Zen.DbAccess.Oracle/bin/Release/Zen.DbAccess.Oracle.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json

set "PROJ_FILE=./Zen.DbAccess.Postgresql/Zen.DbAccess.Postgresql.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./Zen.DbAccess.Postgresql/bin/Release/Zen.DbAccess.Postgresql.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json

set "PROJ_FILE=./Zen.DbAccess.MariaDb/Zen.DbAccess.MariaDb.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./Zen.DbAccess.MariaDb/bin/Release/Zen.DbAccess.MariaDb.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json

set "PROJ_FILE=./Zen.DbAccess.Sqlite/Zen.DbAccess.Sqlite.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./Zen.DbAccess.Sqlite/bin/Release/Zen.DbAccess.Sqlite.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json

set "PROJ_FILE=./Zen.DbAccess.SqlServer/Zen.DbAccess.SqlServer.csproj"

FOR /F "delims=" %%i IN ('%PYTHON_EXE% "%PYTHON_SCRIPT%" "%PROJ_FILE%"') DO (
    set "VERSION=%%i"
)

echo "%VERSION%"

dotnet nuget push ./Zen.DbAccess.SqlServer/bin/Release/Zen.DbAccess.SqlServer.%VERSION%.nupkg --skip-duplicate --api-key %DBACCESS_NUGET_API_KEY% --source https://api.nuget.org/v3/index.json


