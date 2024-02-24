using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.Attributes;
using Zen.DbAccess.Models;

namespace DataAccess.Models;

public class Person : DbModel
{
    [PrimaryKey]
    public int Id { get; set; }
    public string? FirstName { get; set; }
    public string LastName { get; set; }
}
