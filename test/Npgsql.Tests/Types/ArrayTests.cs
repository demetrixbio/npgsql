#region License
// The PostgreSQL License
//
// Copyright (C) 2017 The Npgsql Development Team
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written
// agreement is hereby granted, provided that the above copyright notice
// and this paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE NPGSQL DEVELOPMENT TEAM BE LIABLE TO ANY PARTY
// FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
// INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE NPGSQL DEVELOPMENT TEAM HAS BEEN ADVISED OF
// THE POSSIBILITY OF SUCH DAMAGE.
//
// THE NPGSQL DEVELOPMENT TEAM SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE NPGSQL DEVELOPMENT TEAM HAS NO OBLIGATIONS
// TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Npgsql.Tests.Types
{
    internal static class HelperExtensions
    {
        private static readonly MethodInfo GetFieldValueMethod;

        static HelperExtensions()
        {
            GetFieldValueMethod = typeof(NpgsqlDataReader).GetMethod(nameof(NpgsqlDataReader.GetFieldValue), BindingFlags.Public | BindingFlags.Instance);
        }

        public static object GetFieldValueGeneric(this NpgsqlDataReader reader, Type typeArg, int index)
        {
            return GetFieldValueMethod.MakeGenericMethod(typeArg).Invoke(reader, new object[] { index });
        }

#if NETCOREAPP1_1
        internal delegate TOutput Converter<TInput, TOutput>(TInput input);
#endif
        public static Array ConvertAllMultidimensional<TInput, TOutput>(this Array ary, Converter<TInput, TOutput> converter)
        {
            if (ary.Rank == 1 && ary.GetLowerBound(0) == 0)
                return ary.Cast<TInput>().Select(e => converter(e)).ToArray();

            var lengths = Enumerable.Range(0, ary.Rank).Select(dimension => ary.GetLength(dimension)).ToArray();
            var result = Array.CreateInstance(typeof(TOutput), lengths);
            var indices = new int[lengths.Length];

            indices[0] = -1;
            for (var i = 0; i < lengths.Length;)
            {
                indices[i]++;
                if (indices[i] < lengths[i])
                {
                    result.SetValue(converter((TInput)ary.GetValue(indices)), indices);
                    i = 0;
                }
                else
                {
                    indices[i++] = 0;
                }
            }
            return result;
        }

    }

    /// <summary>
    /// Tests on PostgreSQL arrays
    /// </summary>
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/arrays.html
    /// </remarks>
    class ArrayTests : TestBase
    {
        [Test, Description("Resolves an array type handler via the different pathways")]
        public void ArrayTypeResolution()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString)
            {
                ApplicationName = nameof(ArrayTypeResolution),  // Prevent backend type caching in TypeHandlerRegistry
                Pooling = false
            };

            using (var conn = OpenConnection(csb))
            {
                // Resolve type by NpgsqlDbType
                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    cmd.Parameters.AddWithValue("p", NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.That(reader.GetDataTypeName(0), Is.EqualTo("_int4"));
                    }
                }

                // Resolve type by ClrType (type inference)
                conn.ReloadTypes();
                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Value = new int[0] });
                    using (var reader = cmd.ExecuteReader())
                    {
                        reader.Read();
                        Assert.That(reader.GetDataTypeName(0), Is.EqualTo("_int4"));
                    }
                }

                // Resolve type by OID (read)
                conn.ReloadTypes();
                using (var cmd = new NpgsqlCommand("SELECT '{1, 3}'::INTEGER[]", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetDataTypeName(0), Is.EqualTo("_int4"));
                }
            }
        }

        [Test, Description("Roundtrips a simple, one-dimensional array of ints")]
        public void Ints()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1, @p2, @p3", conn))
            {
                var expected = new[] { 1, 5, 9 };
                var p1 = new NpgsqlParameter("p1", NpgsqlDbType.Array | NpgsqlDbType.Integer);
                var p2 = new NpgsqlParameter { ParameterName = "p2", Value = expected };
                var p3 = new NpgsqlParameter<int[]>("p3", expected);
                cmd.Parameters.Add(p1);
                cmd.Parameters.Add(p2);
                cmd.Parameters.Add(p3);
                p1.Value = expected;
                var reader = cmd.ExecuteReader();
                reader.Read();

                for (var i = 0; i < cmd.Parameters.Count; i++)
                {
                    Assert.That(reader.GetValue(i), Is.EqualTo(expected));
                    Assert.That(reader.GetProviderSpecificValue(i), Is.EqualTo(expected));
                    Assert.That(reader.GetFieldValue<int[]>(i), Is.EqualTo(expected));
                    Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(Array)));
                    Assert.That(reader.GetProviderSpecificFieldType(i), Is.EqualTo(typeof(Array)));
                }
            }
        }

        [Test, Description("Roundtrips a large, one-dimensional array of ints that will be chunked")]
        public void LongOneDimensional()
        {
            using (var conn = OpenConnection())
            {
                var expected = new int[conn.Settings.WriteBufferSize/4 + 100];
                for (var i = 0; i < expected.Length; i++)
                    expected[i] = i;
                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    var p = new NpgsqlParameter {ParameterName = "p", Value = expected};
                    cmd.Parameters.Add(p);
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                    {
                        reader.Read();
                        Assert.That(reader[0], Is.EqualTo(expected));
                    }
                }
            }
        }

        [Test, Description("Roundtrips a large, two-dimensional array of ints that will be chunked")]
        public void LongTwoDimensional()
        {
            using (var conn = OpenConnection())
            {
                var len = conn.Settings.WriteBufferSize/2 + 100;
                var expected = new int[2, len];
                for (var i = 0; i < len; i++)
                    expected[0, i] = i;
                for (var i = 0; i < len; i++)
                    expected[1, i] = i;
                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    var p = new NpgsqlParameter {ParameterName = "p", Value = expected};
                    cmd.Parameters.Add(p);
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                    {
                        reader.Read();
                        Assert.That(reader[0], Is.EqualTo(expected));
                    }
                }
            }
        }

        [Test, Description("Roundtrips a long, one-dimensional array of strings, including a null")]
        public void StringsWithNull()
        {
            using (var conn = OpenConnection())
            {
                var largeString = new StringBuilder();
                largeString.Append('a', conn.Settings.WriteBufferSize);
                var expected = new[] {"value1", null, largeString.ToString(), "val3"};
                using (var cmd = new NpgsqlCommand("SELECT @p", conn))
                {
                    var p = new NpgsqlParameter("p", NpgsqlDbType.Array | NpgsqlDbType.Text) {Value = expected};
                    cmd.Parameters.Add(p);
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SequentialAccess))
                    {
                        reader.Read();
                        Assert.That(reader.GetFieldValue<string[]>(0), Is.EqualTo(expected));
                    }
                }
            }
        }

        [Test, Description("Roundtrips a zero-dimensional array of ints, should return empty one-dimensional")]
        public void ZeroDimensional()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p", conn))
            {
                var expected = new int[0];
                var p = new NpgsqlParameter("p", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = expected };
                cmd.Parameters.Add(p);
                var reader = cmd.ExecuteReader();
                reader.Read();
                Assert.That(reader.GetValue(0), Is.EqualTo(expected));
                Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(expected));
                Assert.That(reader.GetFieldValue<int[]>(0), Is.EqualTo(expected));
                cmd.Dispose();
            }
        }

        [Test, Description("Roundtrips a two-dimensional array of ints")]
        public void TwoDimensionalInts()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn))
            {
                var expected = new[,] { { 1, 2, 3 }, { 7, 8, 9 } };
                var p1 = new NpgsqlParameter("p1", NpgsqlDbType.Array | NpgsqlDbType.Integer);
                var p2 = new NpgsqlParameter { ParameterName = "p2", Value = expected };
                cmd.Parameters.Add(p1);
                cmd.Parameters.Add(p2);
                p1.Value = expected;
                var reader = cmd.ExecuteReader();
                reader.Read();
                Assert.That(reader.GetValue(0), Is.EqualTo(expected));
                Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(expected));
                Assert.That(reader.GetFieldValue<int[,]>(0), Is.EqualTo(expected));
            }
        }

        [Test, Description("Reads a one-dimensional array dates, both as DateTime and as the provider-specific NpgsqlDate")]
        public void ReadProviderSpecificType()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(@"SELECT '{ ""2014-01-04"", ""2014-01-08"" }'::DATE[]", conn))
            {
                var expectedRegular = new[] { new DateTime(2014, 1, 4), new DateTime(2014, 1, 8) };
                var expectedPsv = new[] { new NpgsqlDate(2014, 1, 4), new NpgsqlDate(2014, 1, 8) };
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetValue(0), Is.EqualTo(expectedRegular));
                    Assert.That(reader.GetFieldValue<DateTime[]>(0), Is.EqualTo(expectedRegular));
                    Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(expectedPsv));
                    Assert.That(reader.GetFieldValue<NpgsqlDate[]>(0), Is.EqualTo(expectedPsv));
                }
            }
        }

        [Test, Description("Reads an one-dimensional array with lower bound != 0")]
        public void ReadNonZeroLowerBounded()
        {
            using (var conn = OpenConnection())
            {
                using (var cmd = new NpgsqlCommand("SELECT '[2:3]={ 8, 9 }'::INT[]", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetFieldValue<int[]>(0), Is.EqualTo(new[] {8, 9}));
                }

                using (var cmd = new NpgsqlCommand("SELECT '[2:3][2:3]={ {8,9}, {1,2} }'::INT[][]", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetFieldValue<int[,]>(0), Is.EqualTo(new[,] {{8, 9}, {1, 2}}));
                }
            }
        }

        [Test, Description("Roundtrips a one-dimensional array of bytea values")]
        public void Byteas()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn))
            {
                var expected = new[] { new byte[] { 1, 2 }, new byte[] { 3, 4, } };
                var p1 = new NpgsqlParameter("p1", NpgsqlDbType.Array | NpgsqlDbType.Bytea);
                var p2 = new NpgsqlParameter { ParameterName = "p2", Value = expected };
                cmd.Parameters.Add(p1);
                cmd.Parameters.Add(p2);
                p1.Value = expected;
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetValue(0), Is.EqualTo(expected));
                    Assert.That(reader.GetFieldValue<byte[][]>(0), Is.EqualTo(expected));
                    Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(Array)));
                    Assert.That(reader.GetProviderSpecificFieldType(0), Is.EqualTo(typeof(Array)));
                }
            }
        }

#if !NETCOREAPP1_1
        [Test, Description("Roundtrips a non-generic IList as an array")]
        // ReSharper disable once InconsistentNaming
        public void IListNonGeneric()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p", conn))
            {
                var expected = new ArrayList(new[] { 1, 2, 3 });
                var p = new NpgsqlParameter("p", NpgsqlDbType.Array | NpgsqlDbType.Integer) { Value = expected };
                cmd.Parameters.Add(p);
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(expected.ToArray()));
            }
        }
#endif

        [Test, Description("Roundtrips a generic IList as an array")]
        // ReSharper disable once InconsistentNaming
        public void IListGeneric()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn))
            {
                var expected = new[] {1, 2, 3};
                var p1 = new NpgsqlParameter {ParameterName = "p1", Value = expected.ToList()};
                var p2 = new NpgsqlParameter {ParameterName = "p2", Value = expected.ToList()};
                cmd.Parameters.Add(p1);
                cmd.Parameters.Add(p2);
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader[0], Is.EqualTo(expected.ToArray()));
                    Assert.That(reader[1], Is.EqualTo(expected.ToArray()));
                }
            }
        }

        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/844")]
        public void IEnumerableThrowsFriendlyException()
        {
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1", conn))
            {
                cmd.Parameters.AddWithValue("p1", Enumerable.Range(1, 3));
                Assert.That(() => cmd.ExecuteScalar(), Throws.Exception.TypeOf<NotSupportedException>().With.Message.Contains("use .ToList()/.ToArray() instead"));
            }
        }

#if !NETCOREAPP1_1
        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/960")]
        public void MixedElementTypes()
        {
            var mixedList = new ArrayList { 1, "yo" };
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1", conn))
            {
                cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Array | NpgsqlDbType.Integer, mixedList);
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception
                    .TypeOf<Exception>()
                    .With.Message.Contains("mix"));
            }
        }
#endif

        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/960")]
        public void JaggedArraysNotSupported()
        {
            var jagged = new int[2][];
            jagged[0] = new[] { 8 };
            jagged[1] = new[] { 8, 10 };
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand("SELECT @p1", conn))
            {
                cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Array | NpgsqlDbType.Integer, jagged);
                Assert.That(() => cmd.ExecuteNonQuery(), Throws.Exception
                    .TypeOf<Exception>()
                    .With.Message.Contains("jagged"));
            }
        }

        [Test, Description("Checks that IList<T>s are properly serialized as arrays of their underlying types")]
        public void ListTypeResolution()
        {
            using (var conn = OpenConnection(ConnectionString))
            {
                AssertIListRoundtrips(conn, new[] { 1, 2, 3 });
                AssertIListRoundtrips(conn, new IntList() { 1, 2, 3 });
                AssertIListRoundtrips(conn, new MisleadingIntList<string>() { 1, 2, 3 });
            }
        }

        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1546")]
        public void GenericListGetNpgsqlDbType()
        {
            var p = new NpgsqlParameter
            {
                ParameterName = "p1",
                Value = new List<int> { 1, 2, 3 }
            };
            Assert.That(p.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Array | NpgsqlDbType.Integer));
        }

        void AssertIListRoundtrips<TElement>(NpgsqlConnection conn, IEnumerable<TElement> value)
        {
            using (var cmd = new NpgsqlCommand("SELECT @p", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Value = value });

                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    Assert.That(reader.GetDataTypeName(0), Is.EqualTo("_int4"));
                    Assert.That(reader[0], Is.EqualTo(value.ToArray()));
                }
            }
        }

        class IntList : List<int> { }
        class MisleadingIntList<T> : List<int> { }

        static readonly Dictionary<string, NpgsqlDbType> _npgsqlDbType = new Dictionary<string, NpgsqlDbType>()
        {
            { "bool", NpgsqlDbType.Boolean },
            { "int2", NpgsqlDbType.Smallint },
            { "int4", NpgsqlDbType.Integer },
            { "int8", NpgsqlDbType.Bigint },
            { "float4", NpgsqlDbType.Real },
            { "float8", NpgsqlDbType.Double },
            { "numeric", NpgsqlDbType.Numeric },
            { "money", NpgsqlDbType.Money },
            { "text", NpgsqlDbType.Text },
            { "varchar", NpgsqlDbType.Varchar },
            { "char", NpgsqlDbType.Char },
            { "citext", NpgsqlDbType.Citext },
            { "json", NpgsqlDbType.Json },
            { "jsonb", NpgsqlDbType.Jsonb },
            { "xml", NpgsqlDbType.Xml },
            { "point", NpgsqlDbType.Point },
            { "lseg", NpgsqlDbType.LSeg },
            { "path", NpgsqlDbType.Path },
            { "polygon", NpgsqlDbType.Polygon },
            { "line", NpgsqlDbType.Line },
            { "circle", NpgsqlDbType.Circle },
            { "box", NpgsqlDbType.Box },
            { "bit", NpgsqlDbType.Bit },
            { "bit(1)", NpgsqlDbType.Bit },
            { "varbit", NpgsqlDbType.Varbit },
            { "hstore", NpgsqlDbType.Hstore },
            { "uuid", NpgsqlDbType.Uuid },
            { "cidr", NpgsqlDbType.Cidr },
            { "inet", NpgsqlDbType.Inet },
            { "macaddr", NpgsqlDbType.MacAddr },
            { "tsquery", NpgsqlDbType.TsQuery },
            { "tsvector", NpgsqlDbType.TsVector },
            { "date", NpgsqlDbType.Date },
            { "interval", NpgsqlDbType.Interval },
            { "timestamp", NpgsqlDbType.Timestamp },
            { "timestamptz", NpgsqlDbType.TimestampTz },
            { "time", NpgsqlDbType.Time },
            { "timetz", NpgsqlDbType.TimeTz },
            { "bytea", NpgsqlDbType.Bytea },
            { "oid", NpgsqlDbType.Oid },
            { "xid", NpgsqlDbType.Xid },
            { "cid", NpgsqlDbType.Cid },
            { "oidvector", NpgsqlDbType.Oidvector },
            { "name", NpgsqlDbType.Name },
            { "\"char\"", NpgsqlDbType.InternalChar },
            { "geometry", NpgsqlDbType.Geometry },
            { "regtype", NpgsqlDbType.Regtype },
        };


        private static object[][] OneDimensionalArrayWithNullValues_Tests = new[] {
            new object[] { "bool", typeof(bool?[]), null, new Type[] { }, new Type[] { typeof(bool?[]) }, new bool?[] { false, true, null } },
            new object[] { "int2", typeof(short?[]), null, new Type[] { }, new Type[] { typeof(short?[]) }, new int?[] { 0, 1, null } },
            new object[] { "int4", typeof(int?[]), null, new Type[] { }, new Type[] { typeof(int?[]) }, new int?[] { 0, 1, null } },
            new object[] { "int8", typeof(long?[]), null, new Type[] { }, new Type[] { typeof(long?[]) }, new int?[] { 0, 1, null } },
            new object[] { "float4", typeof(float?[]), null, new Type[] { }, new Type[] { typeof(float?[]) }, new int?[] { 0, 1, null } },
            new object[] { "float8", typeof(double?[]), null, new Type[] { }, new Type[] { typeof(double?[]) }, new int?[] { 0, 1, null } },
            new object[] { "numeric", typeof(decimal?[]), null, new Type[] { }, new Type[] { typeof(decimal?[]) }, new int?[] { 0, 1, null } },
            new object[] { "money", typeof(decimal?[]), null, new Type[] { }, new Type[] { typeof(decimal?[]) }, new int?[] { 0, 1, null } },
            new object[] { "text", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "A", "B", null } },
            new object[] { "varchar", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "A", "B", null } },
            new object[] { "char", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "A", "B", null } },
            new object[] { "citext", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "A", "B", null } },
            new object[] { "json", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "0", "1", null } },
            new object[] { "jsonb", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "0", "1", null } },
            new object[] { "xml", typeof(string[]), null, new Type[] { }, new Type[] { typeof(string[]) }, new [] { "<foo>bar</foo>", "<bar>foo</bar>", null } },
            new object[] { "point", typeof(NpgsqlPoint?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlPoint?[]) }, new NpgsqlPoint?[] { new NpgsqlPoint(0,0), new NpgsqlPoint(1,1), null } },
            new object[] { "lseg", typeof(NpgsqlLSeg?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlLSeg?[]) }, new NpgsqlLSeg?[] { new NpgsqlLSeg(0d,0d,1d,1d), new NpgsqlLSeg(2d,2d,3d,3d), null } },
            new object[] { "path", typeof(NpgsqlPath?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlPath?[]) }, new NpgsqlPath?[] { new NpgsqlPath(new NpgsqlPoint(0,0), new NpgsqlPoint(1,1)), new NpgsqlPath(new NpgsqlPoint(2,2), new NpgsqlPoint(3,3)), null } },
            new object[] { "polygon", typeof(NpgsqlPolygon?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlPolygon?[]) }, new NpgsqlPolygon?[] { new NpgsqlPolygon(new NpgsqlPoint(0,0), new NpgsqlPoint(1,1)), new NpgsqlPolygon(new NpgsqlPoint(2,2), new NpgsqlPoint(3,3)), null } },
            new object[] { "line", typeof(NpgsqlLine?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlLine?[]) }, new NpgsqlLine?[] { new NpgsqlLine(0d,1d,2d), new NpgsqlLine(3d,4d,5d), null } },
            new object[] { "circle", typeof(NpgsqlCircle?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlCircle?[]) }, new NpgsqlCircle?[] { new NpgsqlCircle(0d,0d,1d), new NpgsqlCircle(2d,2d,3d), null } },
            new object[] { "box", typeof(NpgsqlBox?[]), null, new Type[] { }, new Type[] { typeof(NpgsqlBox?[]) }, new NpgsqlBox?[] { new NpgsqlBox(3d,2d,1d,0d), new NpgsqlBox(7d,6d,5d,4d), null } },
            new object[] { "varbit", typeof(BitArray[]), null, new Type[] { }, new Type[] { typeof(BitArray[]) }, new bool[][] { new[] { false, true }, new[] { false, true }, null } },
            new object[] { "hstore", typeof(IDictionary<string, string>[]), null, new Type[] { }, new Type[] { typeof(IDictionary<string, string>[]) }, new [] { new Dictionary<string, string> { { "foo", "bar" } }, new Dictionary<string, string> { { "bar", "foo" } }, null } },
            new object[] { "uuid", typeof(Guid?[]), null, new Type[] { }, new Type[] { typeof(Guid?[]) }, new Guid?[] { Guid.NewGuid(), Guid.NewGuid(), null } },
            //new object[] { "cidr", typeof(NpgsqlInet[]), null, new Type[] { typeof(string[]) }, new Type[] { typeof(IPAddress[]), typeof(NpgsqlInet[]) }, new [] { false, true } },
            //new object[] { "inet", typeof(IPAddress[]), typeof(NpgsqlInet), new Type[] { typeof(string[]) }, new Type[] { typeof(IPAddress[]), typeof(NpgsqlInet[]) }, new [] { false, true } },
            //new object[] { "macaddr", typeof(PhysicalAddress[]), null, new Type[] { typeof(string[]) }, new Type[] { typeof(PhysicalAddress[]) }, new [] { false, true } },
            //new object[] { "tsquery", typeof(NpgsqlTsQuery[]), null, new Type[] {  }, new Type[] { typeof(NpgsqlTsQuery[]) }, new [] { false, true } },
            //new object[] { "tsvector", typeof(NpgsqlTsVector[]), null, new Type[] {  }, new Type[] { typeof(NpgsqlTsVector[]) }, new [] { false, true } },
            //new object[] { "date", typeof(DateTime[]), typeof(NpgsqlDate), new Type[] {  }, new Type[] { typeof(DateTime[]), typeof(NpgsqlDate[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "interval", typeof(TimeSpan[]), typeof(NpgsqlTimeSpan), new Type[] {  }, new Type[] { typeof(TimeSpan[]), typeof(NpgsqlTimeSpan[]), typeof(string[]) }, new [] { false, true } },
            //new object[] { "timestamp", typeof(DateTime[]), typeof(NpgsqlDateTime), new Type[] {  }, new Type[] { typeof(DateTime[]), typeof(DateTimeOffset[]), typeof(NpgsqlDateTime[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "timestamptz", typeof(DateTime[]), typeof(NpgsqlDateTime), new Type[] { typeof(DateTimeOffset[]) }, new Type[] { typeof(DateTime[]), typeof(DateTimeOffset[]), typeof(NpgsqlDateTime[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "time", typeof(TimeSpan[]), null, new Type[] {  }, new Type[] { typeof(TimeSpan[]), typeof(string[]) }, new [] { false, true } },
            //new object[] { "timetz", typeof(DateTimeOffset[]), null, new Type[] { typeof(DateTimeOffset[]), typeof(DateTime[]), typeof(TimeSpan[]) }, new Type[] { typeof(DateTimeOffset[]), typeof(DateTime[]), typeof(TimeSpan[]) }, new [] { false, true } },
            //new object[] { "bytea", typeof(byte[][]), null, new Type[] {  }, new Type[] { typeof(byte[][]), typeof(ArraySegment[]) }, new [] { false, true } },
            //new object[] { "oid", typeof(uint[]), null, new Type[] {  }, new Type[] { typeof(uint[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "xid", typeof(uint[]), null, new Type[] {  }, new Type[] { typeof(uint[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "cid", typeof(uint[]), null, new Type[] {  }, new Type[] { typeof(uint[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "oidvector", typeof(uint[][]), null, new Type[] {  }, new Type[] { typeof(uint[][]) }, new [] { false, true } },
            //new object[] { "name", typeof(string[]), null, new Type[] { typeof(char[][]) }, new Type[] { typeof(string[]), typeof(char[][]), typeof(char[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "char", typeof(char[]), null, new Type[] { typeof(byte[]), typeof(short[]), typeof(int[]), typeof(long[]) }, new Type[] { typeof(byte[]), typeof(IConvertible[]) }, new [] { false, true } },
            //new object[] { "geometry", typeof(PostgisGeometry[]), null, new Type[] {  }, new Type[] { typeof(PostgisGeometry[]) }, new [] { false, true } },
            //new object[] { "bit(1)", typeof(bool[]), null, new Type[] { typeof(BitArray[]) }, new Type[] { typeof(BitArray[]), typeof(bool[]), typeof(string[]) }, new [] { false, true } },
            //new object[] { "bit(2)", typeof(BitArray[]), null, new Type[] {  }, new Type[] { typeof(BitArray[]), typeof(bool[]), typeof(string[]) }, new [] { false, true } },
        };

        [Test, Explicit("Testing of all possible data types is quite extensive"), Description("Roundtrips a one dimensional array that contains null values")]
        [TestCaseSource("OneDimensionalArrayWithNullValues_Tests")]
        public void OneDimensionalArrayWithNullValues(string postgreSQLType, Type defaultType, Type providerSpecificType, Type[] otherOutputTypes, Type[] inputTypes, Array input)
        {
            Type[] allTypes = (new Type[] { defaultType }).Concat(otherOutputTypes).ToArray();
            var defaultElementType = defaultType.GetElementType();

            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery($"CREATE TEMP TABLE mytable(val {postgreSQLType}[]);");
                var insert = new NpgsqlCommand("INSERT INTO mytable(val) VALUES(@p1)", conn);
                foreach (Type t in inputTypes)
                {
                    var inValue = ConvertElements(input, t.GetElementType());
                    insert.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Array | _npgsqlDbType[postgreSQLType]) { Value = inValue });
                    insert.ExecuteNonQuery();
                    insert.Parameters.Clear();

                    // in the following we test if inferring the data type from the .Net type works
                    // which it doesn't for some types by design
                    if( !(postgreSQLType == "json" || postgreSQLType == "jsonb" || postgreSQLType == "xml")
                        &&
                        t == defaultType || Nullable.GetUnderlyingType(t.GetElementType()) == defaultElementType)
                    {
                        insert.Parameters.Add(new NpgsqlParameter { ParameterName = "p1", Value = inValue });
                        insert.ExecuteNonQuery();
                        insert.Parameters.Clear();
                        insert.Parameters.Add(CreateGenericParameter(t, "p1", inValue));
                        insert.ExecuteNonQuery();
                        insert.Parameters.Clear();
                    }
                }

                var select = new NpgsqlCommand("SELECT val FROM mytable", conn);
                using (var reader = select.ExecuteReader(CommandBehavior.SingleResult))
                {
                    Assert.True(reader.Read());
                    do
                    {

                        var expextedDefaultOutValue = ConvertElements(input, defaultType.GetElementType());
                        var value = reader.GetValue(0);
                        ConditionalTypeAssert(value, defaultType);
                        Assert.That(value, Is.EqualTo(expextedDefaultOutValue));

                        var providerSpecificValue = reader.GetProviderSpecificValue(0);
                        if (providerSpecificType != null)
                        {
                            var expextedProviderSpecificOutValue = ConvertElements(input, providerSpecificType.GetElementType());
                            ConditionalTypeAssert(providerSpecificValue, providerSpecificType);
                            Assert.That(providerSpecificValue, Is.EqualTo(expextedProviderSpecificOutValue));
                        }
                        else
                        {
                            ConditionalTypeAssert(providerSpecificValue, defaultType);
                            Assert.That(providerSpecificValue, Is.EqualTo(expextedDefaultOutValue));
                        }

                        foreach (Type t in allTypes)
                        {
                            var expextedOutValue = ConvertElements(input, t.GetElementType());
                            var requestedValue = reader.GetFieldValueGeneric(t, 0);
                            ConditionalTypeAssert(requestedValue, t);
                            Assert.That(requestedValue, Is.EqualTo(expextedOutValue));
                        }
                    } while (reader.Read());
                }
            }
            void ConditionalTypeAssert(object value, Type expectedType)
            {
                if (expectedType.IsInterface || (expectedType.GetElementType()?.IsInterface ?? false))
                    Assert.That(value, Is.AssignableTo(expectedType));
                else
                    Assert.That(value, Is.TypeOf(expectedType));
            }
        }

        private NpgsqlParameter CreateGenericParameter(Type typeArg, string parameterName, object parameterValue)
        {
            var genericParameterType = typeof(NpgsqlParameter<>).MakeGenericType(typeArg);
            var genericParameterConstructor = genericParameterType.GetConstructor(new[] { typeof(string), typeArg });
            return (NpgsqlParameter)genericParameterConstructor.Invoke(new object[] { parameterName, parameterValue });
        }

        private Array ConvertElements(Array source, Type targetType)
        {
            var elementType = source.GetType().GetElementType();
            if (elementType == targetType)
                return source;

            if (typeof(IConvertible).IsAssignableFrom(targetType) ||
                typeof(IConvertible).IsAssignableFrom(Nullable.GetUnderlyingType(targetType)))
            {
                if(typeof(IConvertible).IsAssignableFrom(elementType)||
                    typeof(IConvertible).IsAssignableFrom(Nullable.GetUnderlyingType(elementType)))
                    return ConvertIConvertible(source, targetType);
            }
            if (elementType == typeof(string) && targetType == typeof(char[]))
                return source.ConvertAllMultidimensional<string, char[]>(e => e.ToCharArray());
            if (elementType == typeof(bool[]) && targetType == typeof(BitArray))
                return source.ConvertAllMultidimensional<bool[], BitArray>(e => e == null ? null : new BitArray(e));
            if (elementType == typeof(Dictionary<string, string>) && targetType == typeof(IDictionary<string, string>))
                return source.ConvertAllMultidimensional<Dictionary<string, string>, IDictionary<string, string>>(e => e);

            throw new NotImplementedException();
        }

        private Array ConvertIConvertible(Array source, Type targetType)
        {
            if (targetType == typeof(bool))
                return source.ConvertAllMultidimensional<IConvertible, bool>(e => Convert.ToBoolean(e));
            if (targetType == typeof(bool?))
                return source.ConvertAllMultidimensional<object, bool?>(e => e == null ? (bool?)null : Convert.ToBoolean(e));
            if (targetType == typeof(byte))
                return source.ConvertAllMultidimensional<IConvertible, byte>(e => Convert.ToByte(e));
            if (targetType == typeof(byte?))
                return source.ConvertAllMultidimensional<object, byte?>(e => e == null ? (byte?)null : Convert.ToByte(e));
            if (targetType == typeof(char))
                return source.ConvertAllMultidimensional<IConvertible, char>(e => Convert.ToChar(e));
            if (targetType == typeof(char?))
                return source.ConvertAllMultidimensional<object, char?>(e => e == null ? (char?)null : Convert.ToChar(e));
            if (targetType == typeof(DateTime))
                return source.ConvertAllMultidimensional<IConvertible, DateTime>(e => Convert.ToDateTime(e));
            if (targetType == typeof(DateTime?))
                return source.ConvertAllMultidimensional<object, DateTime?>(e => e == null ? (DateTime?)null : Convert.ToDateTime(e));
            if (targetType == typeof(decimal))
                return source.ConvertAllMultidimensional<IConvertible, decimal>(e => Convert.ToDecimal(e));
            if (targetType == typeof(decimal?))
                return source.ConvertAllMultidimensional<object, decimal?>(e => e == null ? (decimal?)null : Convert.ToDecimal(e));
            if (targetType == typeof(double))
                return source.ConvertAllMultidimensional<IConvertible, double>(e => Convert.ToDouble(e));
            if (targetType == typeof(double?))
                return source.ConvertAllMultidimensional<object, double?>(e => e == null ? (double?)null : Convert.ToDouble(e));
            if (targetType == typeof(short))
                return source.ConvertAllMultidimensional<IConvertible, short>(e => Convert.ToInt16(e));
            if (targetType == typeof(short?))
                return source.ConvertAllMultidimensional<object, short?>(e => e == null ? (short?)null : Convert.ToInt16(e));
            if (targetType == typeof(int))
                return source.ConvertAllMultidimensional<IConvertible, int>(e => Convert.ToInt32(e));
            if (targetType == typeof(int?))
                return source.ConvertAllMultidimensional<object, int?>(e => e == null ? (int?)null : Convert.ToInt32(e));
            if (targetType == typeof(long))
                return source.ConvertAllMultidimensional<IConvertible, long>(e => Convert.ToInt64(e));
            if (targetType == typeof(long?))
                return source.ConvertAllMultidimensional<object, long?>(e => e == null ? (long?)null : Convert.ToInt64(e));
            if (targetType == typeof(sbyte))
                return source.ConvertAllMultidimensional<IConvertible, sbyte>(e => Convert.ToSByte(e));
            if (targetType == typeof(sbyte?))
                return source.ConvertAllMultidimensional<object, sbyte?>(e => e == null ? (sbyte?)null : Convert.ToSByte(e));
            if (targetType == typeof(float))
                return source.ConvertAllMultidimensional<IConvertible, float>(e => Convert.ToSingle(e));
            if (targetType == typeof(float?))
                return source.ConvertAllMultidimensional<object, float?>(e => e == null ? (float?)null : Convert.ToSingle(e));
            if (targetType == typeof(string))
                return source.ConvertAllMultidimensional<object, string>(e => e == null ? null : Convert.ToString(e));
            if (targetType == typeof(ushort))
                return source.ConvertAllMultidimensional<IConvertible, ushort>(e => Convert.ToUInt16(e));
            if (targetType == typeof(ushort?))
                return source.ConvertAllMultidimensional<object, ushort?>(e => e == null ? (ushort?)null : Convert.ToUInt16(e));
            if (targetType == typeof(uint))
                return source.ConvertAllMultidimensional<IConvertible, uint>(e => Convert.ToUInt32(e));
            if (targetType == typeof(uint?))
                return source.ConvertAllMultidimensional<object, uint?>(e => e == null ? (uint?)null : Convert.ToUInt32(e));
            if (targetType == typeof(ulong))
                return source.ConvertAllMultidimensional<IConvertible, ulong>(e => Convert.ToUInt64(e));
            if (targetType == typeof(ulong?))
                return source.ConvertAllMultidimensional<object, ulong?>(e => e == null ? (ulong?)null : Convert.ToUInt64(e));

            // if the target type is simply IConvertible we simply convert so another type
            // that supports IConvertible (preferrably string bit if it's a string we convert it to char[])
            if (source.GetType().GetElementType() == typeof(string))
                return source.ConvertAllMultidimensional<string, char[]>(e => e.ToCharArray());
            return source.ConvertAllMultidimensional<IConvertible, string>(e => e.ToString());
        }
    }
}
