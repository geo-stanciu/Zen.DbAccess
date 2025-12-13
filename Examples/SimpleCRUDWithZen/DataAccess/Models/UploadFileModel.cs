using System;
using System.Collections.Generic;
using System.Text;

namespace DataAccess.Models;

public class UploadFileModel
{
    public long? LongValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public string? TextValue { get; set; }
    public DateTime? DateValue { get; set; }
    public string FileName { get; set; } = null!;
    public byte[] File { get; set; } = null!;
}
