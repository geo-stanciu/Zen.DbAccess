using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;

namespace Zen.DbAccess.Models;

public class DbModel : JsonModel
{
    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal string? dbModel_table { get; set; } = null;

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal DbSqlDeleteModel dbModel_sql_delete { get; set; } = new DbSqlDeleteModel();

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal DbSqlUpdateModel dbModel_sql_update { get; set; } = new DbSqlUpdateModel();

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal DbSqlInsertModel dbModel_sql_insert { get; set; } = new DbSqlInsertModel();

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal HashSet<string>? dbModel_dbColumns { get; set; } = null;

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal List<string>? dbModel_primaryKey_dbColumns { get; set; } = null;

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal Dictionary<string, PropertyInfo>? dbModel_dbColumn_map { get; set; } = null;

    [DbIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    [Newtonsoft.Json.JsonIgnore]
    internal Dictionary<string, string>? dbModel_prop_map { get; set; } = null;

    public void ResetDbModel()
    {
        dbModel_table = null;
        dbModel_sql_delete = new();
        dbModel_sql_update = new();
        dbModel_sql_insert = new();
        dbModel_dbColumns = null;
        dbModel_primaryKey_dbColumns = null;
        dbModel_dbColumn_map = null;
        dbModel_prop_map = null;
    }

    public void CopyDbModelPropsFrom(DbModel model)
    {
        dbModel_table = model.dbModel_table;
        dbModel_sql_delete = model.dbModel_sql_delete;
        dbModel_sql_update = model.dbModel_sql_update;
        dbModel_sql_insert = model.dbModel_sql_insert;
        dbModel_dbColumns = model.dbModel_dbColumns;
        dbModel_primaryKey_dbColumns = model.dbModel_primaryKey_dbColumns;
        dbModel_dbColumn_map = model.dbModel_dbColumn_map;
        dbModel_prop_map = model.dbModel_prop_map;
    }
}
