using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using Zen.DbAccess.Constants;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace Tests.Zen.DbAccess
{
    [TestClass]
    public class SqliteTests : CommonTestSetup
    {
        private readonly string _connStr = "your connection string here";

        public SqliteTests()
            : base()
        {
            _connStr = _config?.GetConnectionString("Sqlite")
                ?? throw new Exception("Invalid or missing configuration file");
        }

        private DbConnectionFactory GetDbConnectionFactory()
        {
            return new DbConnectionFactory(DbConnectionType.Sqlite, _connStr, true, "UTC");
        }

        class T1 : DbModel
        {
            [PrimaryKey]
            public long C1 { get; set; }
            public string? C2 { get; set; }
            public DateTime? C3 { get; set; }
            public DateTime? C4 { get; set; }
            public DateTime? C5 { get; set; }
            public decimal? C6 { get; set; }
        }

        [TestMethod]
        public async Task TestBulkInsertWithPrimaryKeyColumn()
        {
            DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance);

            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync();

            string sql =
                @"create temporary table if not exists test1_t1 (
                      c1 integer primary key,
                      c2 varchar(256),
                      c3 date,
                      c4 timestamp,
                      c5 timestamp with time zone,
                      c6 decimal(18, 4)
                )";

            await sql.ExecuteNonQueryAsync(dbConnectionFactory.DbType, conn);

            List<T1> models = new List<T1>
            {
                new T1 { C1 = 5, C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C1 = 6, C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C1 = 7, C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C1 = 8, C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C1 = 9, C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(dbConnectionFactory.DbType, conn, "test1_t1", insertPrimaryKeyColumn: true);

            sql = "select * from test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);
        }

        [TestMethod]
        public async Task TestBulkInsert()
        {
            DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance);

            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync();

            string sql =
                @"create temporary table if not exists test1_t1 (
                      c1 integer primary key,
                      c2 varchar(256),
                      c3 date,
                      c4 timestamp,
                      c5 timestamp with time zone,
                      c6 decimal(18, 4)
                )";

            await sql.ExecuteNonQueryAsync(dbConnectionFactory.DbType, conn);

            List<T1> models = new List<T1>
            {
                new T1 { C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(dbConnectionFactory.DbType, conn, "test1_t1");

            sql = "select * from test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);
        }

        [TestMethod]
        public async Task TestBulkInsertWithSequence()
        {
            DbConnectionFactory.RegisterDatabaseFactory(DbFactoryNames.SQLITE, SQLiteFactory.Instance);

            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync();

            string sql =
                @"create temporary table if not exists test1_t1 (
                      c1 integer primary key,
                      c2 varchar(256),
                      c3 date,
                      c4 timestamp,
                      c5 timestamp with time zone,
                      c6 decimal(18, 4)
                )";

            await sql.ExecuteNonQueryAsync(dbConnectionFactory.DbType, conn);

            List<T1> models = new List<T1>
            {
                new T1 { C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(dbConnectionFactory.DbType, conn, "test1_t1", sequence2UseForPrimaryKey: "default");

            sql = "select * from test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(dbConnectionFactory.DbType, conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);
        }
    }
}