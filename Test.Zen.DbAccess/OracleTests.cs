using Zen.DbAccess.Extensions;
using Zen.DbAccess.Factories;
using Zen.DbAccess.Shared.Attributes;
using Zen.DbAccess.Shared.Enums;
using Zen.DbAccess.Shared.Models;

namespace Test.Zen.DbAccess
{
    [TestClass]
    public class OracleTests
    {
        private DbConnectionFactory GetDbConnectionFactory()
        {
            string connStr = "Here your Oracle conn string";

            return new DbConnectionFactory(DbConnectionType.Oracle, connStr);
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
                "create sequence test1.t1_seq nocache start with 1";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            sql =
                @"create table test1.t1 (
                      c1 number default on null test1.t1_seq.nextval not null,
                      c2 varchar2(256),
                      c3 date,
                      constraint t1_pk primary key (c1)
                )";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1" },
                new T1 { c2 = "t2", c3 = DateTime.Now.AddDays(1) },
                new T1 { c2 = "t3", c3 = DateTime.Now.AddDays(2) },
                new T1 { c2 = "t4", c3 = DateTime.Now.AddDays(3) },
                new T1 { c2 = "t5", c3 = DateTime.Now.AddDays(4) },
            };

            await models.SaveAllAsync(DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn, conn, "test1.t1").ConfigureAwait(false);

            sql = "drop table test1.t1";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            sql = "drop sequence test1.t1_seq";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);
        }

        [TestMethod]
        public async Task TestBulkInsertWithSequence()
        {
            DbConnectionFactory dbConnectionFactory = GetDbConnectionFactory();

            using var conn = await dbConnectionFactory.BuildAndOpenAsync().ConfigureAwait(false);

            string sql =
                "create sequence test1.t1_seq nocache start with 1";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            sql =
                @"create table test1.t1 (
                      c1 number default on null test1.t1_seq.nextval not null,
                      c2 varchar2(256),
                      c3 date,
                      constraint t1_pk primary key (c1)
                )";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            List<T1> models = new List<T1>
            {
                new T1 { c2 = "t1" },
                new T1 { c2 = "t2", c3 = DateTime.Now.AddDays(1) },
                new T1 { c2 = "t3", c3 = DateTime.Now.AddDays(2) },
                new T1 { c2 = "t4", c3 = DateTime.Now.AddDays(3) },
                new T1 { c2 = "t5", c3 = DateTime.Now.AddDays(4) },
            };

            await models.SaveAllAsync(DbModelSaveType.BulkInsertWithoutPrimaryKeyValueReturn, conn, "test1.t1", sequence2UseForPrimaryKey: "test1.t1_seq").ConfigureAwait(false);

            sql = "drop table test1.t1";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);

            sql = "drop sequence test1.t1_seq";

            await sql.ExecuteNonQueryAsync(conn).ConfigureAwait(false);
        }
    }
}