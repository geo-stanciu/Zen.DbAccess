using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Configuration;
using System.Data;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace Tests.Zen.DbAccess
{
    [TestClass]
    public class SqlServerTests : CommonTestSetup
    {
        private readonly string _connStr = "your connection string here";

        public SqlServerTests()
            : base()
        {
            _connStr = _config?.GetConnectionString("SqlServer") 
                ?? throw new Exception("Invalid or missing configuration file");
        }

        private DbConnectionFactory GetDbConnectionFactory()
        {
            return new DbConnectionFactory(DbConnectionType.SqlServer, _connStr, true, "UTC");
        }

        class T1 : DbModel
        {
            [PrimaryKey]
            public long C1 { get; set; }
            public string? C2 { get; set; }
            public DateTime? C3 { get; set; }
            public DateTime? C4 { get; set; }
            public DateTime? C5 { get; set; }
            public DateTime? C7 { get; set; }
            public decimal? C6 { get; set; }
        }

        [TestMethod]
        public async Task TestBulkInsert()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            await using var conn = await dbConnectionFactory.BuildAsync();

            string sql =
                @"create table #test1_t1 (
                    c1 int identity(1, 1) not null,
                    c2 varchar(256),
                    c3 date,
                    c4 datetime2,
                    c5 datetime2(6),
                    c6 decimal(18, 4),
                    constraint t1_pk primary key (c1)
                )";

            await sql.ExecuteNonQueryAsync(conn);

            sql = "SET IDENTITY_INSERT  #test1_t1 ON ";

            await sql.ExecuteNonQueryAsync(conn);

            List<T1> models = new List<T1>
            {
                new T1 { C1 = 1, C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C1 = 2, C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C1 = 3, C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C1 = 4, C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C1 = 5, C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };
            
            await models.BulkInsertAsync(conn, "#test1_t1", insertPrimaryKeyColumn: true);

            sql = "select * from #test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);

            sql = "drop table #test1_t1";

            await sql.ExecuteNonQueryAsync(conn);
        }

        [TestMethod]
        public async Task TestBulkInsertWithSequence()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            await using var conn = await dbConnectionFactory.BuildAsync();

            string sql =
                @"create table #test1_t1 (
                    c1 int identity(1, 1) not null,
                    c2 varchar(256),
                    c3 date,
                    c4 datetime2,
                    c5 datetime2(6),
                    c6 decimal(18, 4),
                      constraint t1_pk primary key (c1)
                )";

            await sql.ExecuteNonQueryAsync(conn);

            List<T1> models = new List<T1>
            {
                new T1 { C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(conn, "#test1_t1");

            sql = "select * from #test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);

            resultModels[0].C2 = "t212121212";
            resultModels.Add(new T1 { C2 = "t6", C3 = DateTime.UtcNow.AddDays(5), C4 = DateTime.UtcNow.AddDays(5), C5 = DateTime.UtcNow.AddDays(5), C6 = 1234.5678M * 5 });

            await resultModels.SaveAllAsync(conn, "#test1_t1");

            sql = "select * from #test1_t1";

            dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 6);

            sql = "drop table #test1_t1";

            await sql.ExecuteNonQueryAsync(conn);
        }
    }
}