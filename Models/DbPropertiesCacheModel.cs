using System;
using System.Collections.Generic;
using System.Reflection;

namespace Zen.DbAccess.Models;

internal class DbPropertiesCacheModel
{
    internal string? Table { get; set; } = null;

    internal HashSet<string>? DbColumns { get; set; } = null;

    internal Dictionary<string, PropertyInfo>? DbColumnMap { get; set; } = null;

    internal Dictionary<string, string>? PropMap { get; set; } = null;

    internal List<string>? PrimaryKeyDbColumns { get; set; } = null;

    internal List<PropertyInfo> PropertiesToInsert { get; set; } = new List<PropertyInfo>();

    internal List<PropertyInfo> PropertiesToUpdate { get; set; } = new List<PropertyInfo>();

    internal string SqlInsert { get; set; } = string.Empty;

    internal List<SqlParam> SqlInsertParams { get; set; } = new List<SqlParam>();

    internal string SqlUpdate { get; set; } = string.Empty;

    internal List<SqlParam> SqlUpdateParams { get; set; } = new List<SqlParam>();
}
