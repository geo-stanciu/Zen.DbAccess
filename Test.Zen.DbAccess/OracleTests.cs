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
                new T1 { C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M, C7 = DateTime.UtcNow },
                new T1 { C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M, C7 = DateTime.UtcNow.AddDays(1) },
                new T1 { C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2, C7 = DateTime.UtcNow.AddDays(2) },
                new T1 { C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3, C7 = DateTime.UtcNow.AddDays(3) },
                new T1 { C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4, C7 = DateTime.UtcNow.AddDays(4) },
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
                      c1 number not null,
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
                new T1 { C2 = "t1", C4 = DateTime.UtcNow, C5 = DateTime.UtcNow, C6 = 1234.5678M },
                new T1 { C2 = "t2", C3 = DateTime.UtcNow.AddDays(1), C4 = DateTime.UtcNow.AddDays(1), C5 = DateTime.UtcNow.AddDays(1), C6 = 1234.5678M },
                new T1 { C2 = "t3", C3 = DateTime.UtcNow.AddDays(2), C4 = DateTime.UtcNow.AddDays(2), C5 = DateTime.UtcNow.AddDays(2), C6 = 1234.5678M * 2 },
                new T1 { C2 = "t4", C3 = DateTime.UtcNow.AddDays(3), C4 = DateTime.UtcNow.AddDays(3), C5 = DateTime.UtcNow.AddDays(3), C6 = 1234.5678M * 3 },
                new T1 { C2 = "t5", C3 = DateTime.UtcNow.AddDays(4), C4 = DateTime.UtcNow.AddDays(4), C5 = DateTime.UtcNow.AddDays(4), C6 = 1234.5678M * 4 },
            };

            await models.BulkInsertAsync(conn, "test1.t1", sequence2UseForPrimaryKey: "test1.t1_seq");

            sql = "select * from test1.t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);

            var resultModels = await sql.QueryAsync<T1>(conn);

            Assert.IsNotNull(resultModels);
            Assert.IsTrue(resultModels.Count == 5);

            resultModels[0].C2 = "t212121212";
            resultModels.Add(new T1 { C2 = "t6", C3 = DateTime.UtcNow.AddDays(5), C4 = DateTime.UtcNow.AddDays(5), C5 = DateTime.UtcNow.AddDays(5), C6 = 1234.5678M * 5 });

            await resultModels.SaveAllAsync(conn, "test1.t1", sequence2UseForPrimaryKey: "test1.t1_seq");

            sql = "select * from test1.t1";

            dt = await sql.QueryDataTableAsync(conn);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 6);

            sql = "drop table test1.t1";

            await sql.ExecuteNonQueryAsync(conn);

            sql = "drop sequence test1.t1_seq";

            await sql.ExecuteNonQueryAsync(conn);
        }
    }
}