using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Zen.DbAccess.Helpers;

internal static class Sha256Helper
{
    public static string Sha256(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        StringBuilder Sb = new StringBuilder();

        using (var hash = SHA256.Create())
        {
            Encoding enc = Encoding.UTF8;
            byte[] result = hash.ComputeHash(enc.GetBytes(value));

            foreach (byte b in result)
                Sb.Append(b.ToString("x2"));
        }

        return Sb.ToString();
    }

}
