using DataAccess.Models;
using SimpleCRUDWithZen.Models;

namespace SimpleCRUDWithZen.Extensions;

public static class UploadMultiPartDataModelExtensions
{
    public static async Task<UploadFileModel> ToUploadFileModelAsync(this UploadMultiPartDataModel model)
    {
        var fileUpload = new UploadFileModel
        {
            LongValue = model.LongValue,
            DecimalValue = model.DecimalValue,
            TextValue = model.TextValue,
            DateValue = model.DateValue.HasValue ? model.DateValue.Value.ToUniversalTime() : null,
            CreatedAt = DateTime.UtcNow,
        };

        (fileUpload.FileName, fileUpload.File) = await model.File.ReadFileAsync();

        return fileUpload;
    }
}
