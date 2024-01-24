using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Models;

public class ResponseModel : DbModel
{
    public bool is_error { get; set; } = false;
    public string error_message { get; set; } = string.Empty;
}
