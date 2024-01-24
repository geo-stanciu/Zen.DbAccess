using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Models;

public class DbSqlUpdateModel
{
    public string sql_query { get; set; } = string.Empty;
    public List<SqlParam> sql_parameters { get; set; } = new List<SqlParam>();
}
