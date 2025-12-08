using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Enums;

public enum DbConnectionType
{
    SqlServer = 0,
    Oracle,
    Postgresql,
    Sqlite,
    MariaDb
}
