using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Zen.DbAccess.Models;

namespace Zen.DbAccess.DatabaseSpeciffic;

public interface IDbSpeciffic
{
    void PrepareDataAdapter(DbDataAdapter? da);

    object GetValueAsClob(DbConnection conn, object value);

    object GetValueAsBlob(DbConnection conn, object value);

    void DisposeClob(DbCommand cmd, SqlParam prm);

    void DisposeBlob(DbCommand cmd, SqlParam prm);
}
