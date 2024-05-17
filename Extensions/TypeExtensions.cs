using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Extensions;

internal static class TypeExtensions
{
    private static IEnumerable<MethodInfo> GetExtensionMethods(Assembly assembly, Type extendedType, string MethodeName)
    {
        List<MethodInfo> extensionMethods = new List<MethodInfo>();

        foreach (Type t in assembly.GetTypes())
        {
            if (t.IsDefined(typeof(ExtensionAttribute), false))
            {
                foreach (MethodInfo mi in t.GetMethods())
                {
                    if (mi.Name != MethodeName)
                        continue;

                    if (mi.IsDefined(typeof(ExtensionAttribute), false))
                    {
                        var parameters = mi.GetParameters();

                        if (parameters.Any()
                            && (parameters[0].ParameterType == extendedType || extendedType.IsSubclassOf(parameters[0].ParameterType)))
                        {
                            extensionMethods.Add(mi);
                        }
                    }
                }
            }
        }

        return extensionMethods;
    }

    public static MethodInfo? GetExtensionMethod(this Type t, string MethodeName, params Type[] parameters)
    {
        Assembly thisAssembly = typeof(TypeExtensions).Assembly;

        var mi = GetExtensionMethods(thisAssembly, t, MethodeName);

        if (!mi.Any())
            return null;

        if (parameters.Length == 0)
            return mi.First();

        foreach (var m in mi)
        {
            var methodParams = m.GetParameters();

            if (methodParams.Length - parameters.Length != 1) // first is DbModel because it's an extension method
                continue;

            bool found = true;

            for (int i = 1; i < methodParams.Length; i++)
            {
                if (methodParams[i].ParameterType != parameters[i - 1])
                {
                    found = false;
                    break;
                }
            }

            if (found)
                return m;
        }

        return null;
    }
}
