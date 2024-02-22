using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Zen.DbAccess.Converters;

namespace Zen.DbAccess.Models;

public class JsonModel
{
    public string ToJson()
    {
        var options = new JsonSerializerOptions();
        options.Converters.Add(new RuntimeTypeJsonConverter<object>());

        var json = JsonSerializer.Serialize(this, GetType(), options);

        return json;
    }

    public override string ToString()
    {
        return ToJson();
    }
}
