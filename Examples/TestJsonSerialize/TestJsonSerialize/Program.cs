// See https://aka.ms/new-console-template for more information
using System.Text.Json;
using System.Text.Json.Serialization;

Console.WriteLine("Hello, World!");

c0 x = new c1
{
    Prop1 = 1,
    Prop2 = "p2",
    Prop3 = new DateTime(2024, 1, 2),
    Prop4 = new List<c2>
    {
        new c2 {
            Prop11 = 11,
            Prop21 = "p22",
            Prop31 = new DateTime(2024,2,3),
            Prop41 = new List<c3>
            {
                new c3
                {
                    Prop12 = 12,
                    Prop22 = "p22",
                    Prop32 = new DateTime(2024,4,5)
                }
            }
        },
        new c2
        {
            Prop11 = 21,
            Prop21 = "p21",
            Prop31 = new DateTime(2024,6,7),
            Prop41 = new List<c3>()
        }
    }
};

string json = JsonSerializer.Serialize(x);

Console.WriteLine(json);

try
{
    var x2 = JsonSerializer.Deserialize<c1>(json);
}
catch (Exception ex)
{ 
}

[JsonPolymorphic]
class c0
{
    public int Prop0 { get; set; } = 0;
}

class c1 : c0
{
    public int Prop1 { get; set; }
    public string Prop2 { get; set; }
    public DateTime Prop3 { get; set; }
    public List<c2> Prop4 { get; set; }
}

class c2
{
    public int Prop11 { get; set; }
    public string Prop21 { get; set; }
    public DateTime Prop31 { get; set; }

    public List<c3> Prop41 { get; set; }
}

class c3
{
    public int Prop12 { get; set; }
    public string Prop22 { get; set; }
    public DateTime Prop32 { get; set; }
}