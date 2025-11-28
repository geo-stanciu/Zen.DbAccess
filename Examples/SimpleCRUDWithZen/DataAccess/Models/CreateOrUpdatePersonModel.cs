using DataAccess.Enum;
using System;
using System.Collections.Generic;
using System.Text;

namespace DataAccess.Models;

public class CreateOrUpdatePersonModel
{
    public string? FirstName { get; set; }
    public required string LastName { get; set; } = null!;
    public DateOnly? BirthDate { get; set; }
    public PersonTypes? Type { get; set; }
    public byte[]? Image { get; set; }

    public Person ToPerson()
    {
        return new Person
        {
            FirstName = this.FirstName,
            LastName = this.LastName,
            BirthDate = this.BirthDate,
            Type = this.Type,
            Image = this.Image,
        };
    }
}