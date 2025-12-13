namespace SimpleCRUDWithZen.Extensions;

public static class IFormFileExtensions
{
    public static async Task<(string fileName, byte[] content)> ReadFileAsync(this IFormFile formFile)
    {
        using var ms = new MemoryStream();
        await formFile.CopyToAsync(ms);

        ms.Seek(0L, SeekOrigin.Begin);

        var fileName = formFile.FileName;
        var fileContent = ms.ToArray();

        return (fileName, fileContent);
    }
}
