using System;
using System.Collections.Generic;
using System.Reflection;

namespace Zen.DbAccess.Models;

public class DbPropertiesCacheModel
{
    public string? Table { get; internal set; } = null;

    public HashSet<string>? DbColumns { get; internal set; } = null;

    public Dictionary<string, PropertyInfo>? DbColumnMap { get; internal set; } = null;

    public Dictionary<string, string>? PropMap { get; internal set; } = null;

    public List<string>? PrimaryKeyDbColumns { get; internal set; } = null;

    public List<PropertyInfo> PropertiesToInsert { get; internal set; } = new List<PropertyInfo>();

    public List<PropertyInfo> PropertiesToUpdate { get; internal set; } = new List<PropertyInfo>();

    public string SqlInsert { get; internal set; } = string.Empty;

    public List<SqlParam> SqlInsertParams { get; internal set; } = new List<SqlParam>();

    public string SqlUpdate { get; internal set; } = string.Empty;

    public List<SqlParam> SqlUpdateParams { get; internal set; } = new List<SqlParam>();

    public bool HasBulkInsertEnabled { get; set; } = true;
}
