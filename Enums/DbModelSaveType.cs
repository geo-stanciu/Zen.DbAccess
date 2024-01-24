using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Enums;

public enum DbModelSaveType
{
    InsertUpdate = 0,
    InsertOnly,
    BulkInsertWithoutPrimaryKeyValueReturn
}
