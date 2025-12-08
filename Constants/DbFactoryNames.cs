using System;
using System.Collections.Generic;
using System.Text;

namespace Zen.DbAccess.Constants;

public static class DbFactoryNames
{
    public const string SQL_SERVER = "Microsoft.Data.SqlClient";
    public const string ORACLE = "Oracle.ManagedDataAccess.Client";
    public const string POSTGRESQL = "Npgsql";
    public const string SQLITE = "Microsoft.Data.Sqlite";
    public const string MARIADB = "MySql.Data.MySqlClient";
}
