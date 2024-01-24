using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.ContractResolvers;

public class JsonModelContractResolver : DefaultContractResolver
{
    protected override List<MemberInfo> GetSerializableMembers(Type objectType)
    {
        List<MemberInfo> members = base.GetSerializableMembers(objectType);

        //Type avroSchemaType = typeof(Avro.Schema);

        for (int i = members.Count - 1; i >= 0; i--)
        {
            MemberInfo member = members[i];

            //if (member.MemberType == MemberTypes.Property && ((PropertyInfo)member).PropertyType == avroSchemaType)
            //{
            //    members.RemoveAt(i);
            //    continue;
            //}

            object[] attrs = member.GetCustomAttributes(true);

            if (attrs != null
                && attrs.Any(x => x is Newtonsoft.Json.JsonIgnoreAttribute || x is System.Text.Json.Serialization.JsonIgnoreAttribute))
            {
                members.RemoveAt(i);
            }
        }

        return members;
    }
}
