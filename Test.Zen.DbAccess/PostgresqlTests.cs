using System.Data;
using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Shared.Attributes;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Test.Zen.DbAccess
{
    [TestClass]
    public class PostgresqlTests
    {
        private DbConnectionFactory GetDbConnectionFactory()
        {
            string connStr = "Here your Postgresql conn string";

            return new DbConnectionFactory(DbConnectionType.Postgresql, connStr);
        }

        class T1 : DbModel
        {
            [PrimaryKey]
            public long c1 { get; set; }
            public string? c2 { get; set; }
            public DateTime? c3 { get; set; }
        }

        [TestMethod]
        public async Task TestBulkInsert()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);

            string sql =
                @"create temporary table if not exists test1_t1 (
                      c1 serial not null,
                      c2 varchar(256),
                      c3 date,
                      constraint t1_pk primary key (c1)
                ) on commit preserve rows";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1" },
                new T1 { c2 = "t2", c3 = DateTime.Now.AddDays(1) },
                new T1 { c2 = "t3", c3 = DateTime.Now.AddDays(2) },
                new T1 { c2 = "t4", c3 = DateTime.Now.AddDays(3) },
                new T1 { c2 = "t5", c3 = DateTime.Now.AddDays(4) },
            };

            await models.SaveAllAsync(DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn, conn, "test1_t1").ConfigureAwait(false);

            sql = "select * from test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn).ConfigureAwait(false);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);
        }

        [TestMethod]
        public async Task TestBulkInsertWithSequence()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);

            string sql =
                @"create temporary table if not exists test1_t1 (
                      c1 serial not null,
                      c2 varchar(256),
                      c3 date,
                      constraint t1_pk primary key (c1)
                ) on commit preserve rows";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1" },
                new T1 { c2 = "t2", c3 = DateTime.Now.AddDays(1) },
                new T1 { c2 = "t3", c3 = DateTime.Now.AddDays(2) },
                new T1 { c2 = "t4", c3 = DateTime.Now.AddDays(3) },
                new T1 { c2 = "t5", c3 = DateTime.Now.AddDays(4) },
            };

            await models.SaveAllAsync(DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn, conn, "test1_t1", sequence2UseForPrimaryKey: "default").ConfigureAwait(false);

            sql = "select * from test1_t1";

            DataTable? dt = await sql.QueryDataTableAsync(conn).ConfigureAwait(false);

            Assert.IsNotNull(dt);
            Assert.IsTrue(dt.Rows.Count == 5);
        }
    }
}