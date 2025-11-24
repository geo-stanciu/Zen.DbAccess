using DataAccess.Enum;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAccess.Models;

public class CreateOrUpdatePersonModel
{
    public string? FirstName { get; set; }
    public string LastName { get; set; }
    public PersonTypes? Type { get; set; }

    public Person ToPerson()
    {
        return new Person
        {
            FirstName = this.FirstName,
            LastName = this.LastName,
            Type = this.Type
        };
    }
}