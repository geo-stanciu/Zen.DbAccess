using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Zen.DbAccess.Models;

public class JsonModel
{
    public string ToJson()
    {
        var json = JsonConvert.SerializeObject(this);

        return json;
    }

    public override string ToString()
    {
        return ToJson();
    }
}
