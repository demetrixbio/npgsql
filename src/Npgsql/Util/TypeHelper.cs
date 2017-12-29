using System;
using System.Reflection;

namespace Npgsql.Util
{
    internal static class TypeHelper
    {
        public static bool IsValueType(Type t)
        {
#if NETSTANDARD1_3
            return t.GetTypeInfo().IsValueType;
#else
            return t.IsValueType;
#endif
        }

        public static bool IsGenericType(Type t)
        {
#if NETSTANDARD1_3
            return t.GetTypeInfo().IsGenericType;
#else
            return t.IsGenericType;
#endif
        }
    }
}
