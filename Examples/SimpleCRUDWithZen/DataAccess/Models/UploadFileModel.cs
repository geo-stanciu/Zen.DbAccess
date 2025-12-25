using System;
using System.Collections.Generic;
using System.Text;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Models;

namespace DataAccess.Models;

public class UploadFileModel : DbModel
{
    [PrimaryKey]
    public int Id { get; set; } = -1;
    public long? LongValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public string? TextValue { get; set; }
    public DateTime? DateValue { get; set; }
    public string FileName { get; set; } = null!;

    [DbName("file")]
    [DbName("\"FILE\"", DbConnectionType.Oracle, DbConnectionType.SqlServer)]
    public byte[] File { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
}
