using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Zen.DbAccess.Constants;

public static class DbAccessConstants
{
    public const int MaxQueryPropertiesCache = 500_000;

    public const int MaxQueryPropertiesCacheCleanupCount = 10_000;
}