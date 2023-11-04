using Microsoft.Extensions.Configuration;
using System.Data;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Shared.Attributes;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Test.Zen.DbAccess
{
    [TestClass]
    public class OracleTests : CommonTestSetup
    {
        private readonly string _connStr = "your connection string here";

        public OracleTests()
            : base()
        {
            _connStr = _config?.GetConnectionString("Oracle")
                ?? throw new Exception("Invalid or missing configuration file");
        }

        private DbConnectionFactory GetDbConnectionFactory()
        {
            return new DbConnectionFactory(DbConnectionType.Oracle, _connStr, true, "UTC");
        }

        class T1 : DbModel
        {
            [PrimaryKey]
            public long c1 { get; set; }
            public string? c2 { get; set; }
            public DateTime? c3 { get; set; }
            public DateTime? c4 { get; set; }
            public DateTime? c5 { get; set; }
            public DateTime? c7 { get; set; }
            public decimal? c6 { get; set; }
        }

        [TestMethod]
        public async Task TestBulkInsert()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync();

            string sql =
                "create sequence test1.t1_seq nocache start with 1";

            await sql.ExecuteNonQueryAsync(conn);

            sql =
                @"create table test1.t1 (
                    c1 number default on null test1.t1_seq.nextval not null,
                    c2 varchar2(256),
                    c3 date,
                    c4 timestamp,
                    c5 timestamp with time zone,
                    c6 decimal(18, 4),
                    c7 timestamp with local time zone,
                    constraint t1_pk primary key (c1)
            )";

            await sql.ExecuteNonQueryAsync(conn);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1", c4 = DateTime.UtcNow, c5 = DateTime.UtcNow, c6 = 1234.5678M, c7 = DateTime.UtcNow },
                new T1 { c2 = "t2", c3 = DateTime.UtcNow.AddDays(1), c4 = DateTime.UtcNow.AddDays(1), c5 = DateTime.UtcNow.AddDays(1), c6 = 1234.5678M, c7 = DateTime.UtcNow.AddDays(1) },
                new T1 { c2 = "t3", c3 = DateTime.UtcNow.AddDays(2), c4 = DateTime.UtcNow.AddDays(2), c5 = DateTime.UtcNow.AddDays(2), c6 = 1234.5678M * 2, c7 = DateTime.UtcNow.AddDays(2) },
                new T1 { c2 = "t4", c3 = DateTime.UtcNow.AddDays(3), c4 = DateTime.UtcNow.AddDays(3), c5 = DateTime.UtcNow.AddDays(3), c6 = 1234.5678M * 3, c7 = DateTime.UtcNow.AddDays(3) },
                new T1 { c2 = "t5", c3 = DateTime.UtcNow.AddDays(4), c4 = DateTime.UtcNow.AddDays(4), c5 = DateTime.UtcNow.AddDays(4), c6 = 1234.5678M * 4, c7 = DateTime.UtcNow.AddDays(4) },
            };

            await models.BulkInsertAsync(conn, "test1.t1");

            sql = "select * from test1.t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);

            sql = "drop table test1.t1";

            await sql.ExecuteNonQueryAsync(conn);

            sql = "drop sequence test1.t1_seq";

            await sql.ExecuteNonQueryAsync(conn);
        }

        [TestMethod]
        public async Task TestBulkInsertWithSequence()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync();

            string sql =
                "create sequence test1.t1_seq nocache start with 1";

            await sql.ExecuteNonQueryAsync(conn);

            sql =
                @"create table test1.t1 (
                      c1 number default on null test1.t1_seq.nextval not null,
                      c2 varchar2(256),
                      c3 date,
                      c4 timestamp,
                      c5 timestamp with time zone,
                      c6 decimal(18, 4),
                      constraint t1_pk primary key (c1)
                )";

            await sql.ExecuteNonQueryAsync(conn);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1", c4 = DateTime.UtcNow, c5 = DateTime.UtcNow, c6 = 1234.5678M },
                new T1 { c2 = "t2", c3 = DateTime.UtcNow.AddDays(1), c4 = DateTime.UtcNow.AddDays(1), c5 = DateTime.UtcNow.AddDays(1), c6 = 1234.5678M },
                new T1 { c2 = "t3", c3 = DateTime.UtcNow.AddDays(2), c4 = DateTime.UtcNow.AddDays(2), c5 = DateTime.UtcNow.AddDays(2), c6 = 1234.5678M * 2 },
                new T1 { c2 = "t4", c3 = DateTime.UtcNow.AddDays(3), c4 = DateTime.UtcNow.AddDays(3), c5 = DateTime.UtcNow.AddDays(3), c6 = 1234.5678M * 3 },
                new T1 { c2 = "t5", c3 = DateTime.UtcNow.AddDays(4), c4 = DateTime.UtcNow.AddDays(4), c5 = DateTime.UtcNow.AddDays(4), c6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(conn, "test1.t1", sequence2UseForPrimaryKey: "test1.t1_seq");

            sql = "select * from test1.t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);

            sql = "drop table test1.t1";

            await sql.ExecuteNonQueryAsync(conn);

            sql = "drop sequence test1.t1_seq";

            await sql.ExecuteNonQueryAsync(conn);
        }
    }
}