using DataAccess.Models;

namespace SimpleCRUDWithZen.Models;

public class UploadMultiPartDataModel
{
    public long? LongValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public string? TextValue { get; set; }
    public DateTime? DateValue { get; set; }
    public required IFormFile File { get; set; } = null!;
}
