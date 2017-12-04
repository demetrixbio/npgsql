﻿#region License
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

#if !NETCOREAPP1_1

using System;
using System.Data;
using NpgsqlTypes;
using NUnit.Framework;
using Npgsql.PostgresTypes;

namespace Npgsql.Tests
{
    class CommandBuilderTests : TestBase
    {
        [Test, Description("Tests function parameter derivation with IN, OUT and INOUT parameters")]
        public void DeriveParametersVarious()
        {
            using (var conn = OpenConnection())
            {
                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
                    CREATE OR REPLACE FUNCTION pg_temp.func(IN param1 INT, OUT param2 text, INOUT param3 INT) RETURNS record AS
                    '
                    BEGIN
                            param2 = ''sometext'';
                            param3 = param1 + param3;
                    END;
                    ' LANGUAGE 'plpgsql';
                ");

                var cmd = new NpgsqlCommand("pg_temp.func", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(3));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Integer));
                Assert.That(cmd.Parameters[0].PostgresType.Name, Is.EqualTo("int4"));
                Assert.That(cmd.Parameters[0].SpecificType, Is.Null);
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[1].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Text));
                Assert.That(cmd.Parameters[1].PostgresType.Name, Is.EqualTo("text"));
                Assert.That(cmd.Parameters[1].SpecificType, Is.Null);
                Assert.That(cmd.Parameters[2].Direction, Is.EqualTo(ParameterDirection.InputOutput));
                Assert.That(cmd.Parameters[2].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Integer));
                Assert.That(cmd.Parameters[2].PostgresType.Name, Is.EqualTo("int4"));
                Assert.That(cmd.Parameters[2].SpecificType, Is.Null);
                cmd.Parameters[0].Value = 5;
                cmd.Parameters[2].Value = 4;
                cmd.ExecuteNonQuery();
                Assert.That(cmd.Parameters[0].Value, Is.EqualTo(5));
                Assert.That(cmd.Parameters[1].Value, Is.EqualTo("sometext"));
                Assert.That(cmd.Parameters[2].Value, Is.EqualTo(9));
            }
        }

        [Test, Description("Tests function parameter derivation with IN-only parameters")]
        public void DeriveParametersInOnly()
        {
            using (var conn = OpenConnection())
            {
                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
                    CREATE OR REPLACE FUNCTION pg_temp.func(IN param1 INT, IN param2 INT) RETURNS int AS
                    '
                    BEGIN
                    RETURN param1 + param2;
                    END;
                    ' LANGUAGE 'plpgsql';
                ");

                var cmd = new NpgsqlCommand("pg_temp.func", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(2));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Input));
                cmd.Parameters[0].Value = 5;
                cmd.Parameters[1].Value = 4;
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(9));
            }
        }

        [Test, Description("Tests function parameter derivation with no parameters")]
        public void DeriveParametersNoParams()
        {
            using (var conn = OpenConnection())
            {
                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
                    CREATE OR REPLACE FUNCTION pg_temp.func() RETURNS int AS
                    '
                    BEGIN
                    RETURN 4;
                    END;
                    ' LANGUAGE 'plpgsql';
                ");

                var cmd = new NpgsqlCommand("pg_temp.func", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Is.Empty);
            }
        }

        [Test]
        public void FunctionCaseSensitiveNameDeriveParameters()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery(
                    @"CREATE OR REPLACE FUNCTION pg_temp.""FunctionCaseSensitive""(int4, text) returns int4 as
                              $BODY$
                              begin
                                return 0;
                              end
                              $BODY$
                              language 'plpgsql';");
                var command = new NpgsqlCommand("pg_temp.\"FunctionCaseSensitive\"", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(command);
                Assert.AreEqual(NpgsqlDbType.Integer, command.Parameters[0].NpgsqlDbType);
                Assert.AreEqual(NpgsqlDbType.Text, command.Parameters[1].NpgsqlDbType);
            }
        }

        [Test]
        public void DeriveParametersWithParameterNameFromFunction()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery(@"CREATE OR REPLACE FUNCTION pg_temp.testoutparameter2(x int, y int, out sum int, out product int) as 'select $1 + $2, $1 * $2' language 'sql';");
                var command = new NpgsqlCommand("pg_temp.testoutparameter2", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(command);
                Assert.AreEqual("x", command.Parameters[0].ParameterName);
                Assert.AreEqual("y", command.Parameters[1].ParameterName);
            }
        }

        [Test]
        public void DeriveInvalidFunction()
        {
            using (var conn = OpenConnection())
            {
                var invalidCommandName = new NpgsqlCommand("invalidfunctionname", conn) { CommandType = CommandType.StoredProcedure };
                Assert.That(() => NpgsqlCommandBuilder.DeriveParameters(invalidCommandName),
                    Throws.Exception.TypeOf<PostgresException>()
                        .With.Property(nameof(PostgresException.SqlState)).EqualTo("42883"));
            }
        }

        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/1212")]
        public void TableParameters()
        {
            using (var conn = OpenConnection())
            {
                TestUtil.MinimumPgVersion(conn, "9.2.0");

                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
                    CREATE FUNCTION pg_temp.func(IN in1 INT) RETURNS TABLE(t1 INT, t2 INT) AS
                      'SELECT in1,in1+1' LANGUAGE 'sql';
                ");

                var cmd = new NpgsqlCommand("pg_temp.func", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(3));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[2].Direction, Is.EqualTo(ParameterDirection.Output));
                cmd.Parameters[0].Value = 5;
                cmd.ExecuteNonQuery();
                Assert.That(cmd.Parameters[1].Value, Is.EqualTo(5));
                Assert.That(cmd.Parameters[2].Value, Is.EqualTo(6));
            }
        }

        [Test, Description("Tests function parameter derivation for quoted functions with double quotes in the name works")]
        public void FunctionQuoteCharactersInNameDeriveParameters()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery(
                    @"CREATE OR REPLACE FUNCTION pg_temp.""""""FunctionQuote""""CharactersInName""""""(int4, text) returns int4 as
                              $BODY$
                              begin
                                return 0;
                              end
                              $BODY$
                              language 'plpgsql';");
                var command = new NpgsqlCommand("pg_temp.\"\"\"FunctionQuote\"\"CharactersInName\"\"\"", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(command);
                Assert.AreEqual(NpgsqlDbType.Integer, command.Parameters[0].NpgsqlDbType);
                Assert.AreEqual(NpgsqlDbType.Text, command.Parameters[1].NpgsqlDbType);
            }
        }

        [Test, Description("Tests function parameter derivation for quoted functions with dots in the name works")]
        public void FunctionDotCharacterInNameDeriveParameters()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery(
                    @"CREATE OR REPLACE FUNCTION pg_temp.""My.Dotted.Function""(int4, text) returns int4 as
                              $BODY$
                              begin
                                return 0;
                              end
                              $BODY$
                              language 'plpgsql';");
                var command = new NpgsqlCommand("pg_temp.\"My.Dotted.Function\"", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(command);
                Assert.AreEqual(NpgsqlDbType.Integer, command.Parameters[0].NpgsqlDbType);
                Assert.AreEqual(NpgsqlDbType.Text, command.Parameters[1].NpgsqlDbType);
            }
        }

        [Test, Description("Tests if the right function according to search_path is used in function parameter derivation")]
        public void DeriveParametersWithCorrectSchemaResolution()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1, schema2 CASCADE; CREATE SCHEMA schema1; CREATE SCHEMA schema2;");
                try
                {
                    conn.ExecuteNonQuery(
                        @"
CREATE OR REPLACE FUNCTION schema1.redundantfunc() RETURNS int AS
$BODY$
BEGIN
    RETURN 1;
END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION schema2.redundantfunc(IN param1 INT, IN param2 INT) RETURNS int AS
$BODY$
BEGIN
RETURN param1 + param2;
END;
$BODY$
LANGUAGE 'plpgsql';

SET search_path TO schema2;
");
                    var command = new NpgsqlCommand("redundantfunc", conn) { CommandType = CommandType.StoredProcedure };
                    NpgsqlCommandBuilder.DeriveParameters(command);
                    Assert.That(command.Parameters, Has.Count.EqualTo(2));
                    Assert.That(command.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                    Assert.That(command.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Input));
                    command.Parameters[0].Value = 5;
                    command.Parameters[1].Value = 4;
                    Assert.That(command.ExecuteScalar(), Is.EqualTo(9));
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                        conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1, schema2 CASCADE");
                }
            }
        }

        [Test, Description("Tests if function parameter derivation throws an exception if the specified function is not in the search_path")]
        public void DeriveThrowsForExistingFunctionThatIsNotInSearchPath()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1 CASCADE; CREATE SCHEMA schema1;");
                try
                {
                    conn.ExecuteNonQuery(@"
CREATE OR REPLACE FUNCTION schema1.schema1func() RETURNS int AS
$BODY$
BEGIN
    RETURN 1;
END;
$BODY$
LANGUAGE 'plpgsql';

RESET search_path;
");
                    var command = new NpgsqlCommand("schema1func", conn) { CommandType = CommandType.StoredProcedure };
                    Assert.That(() => NpgsqlCommandBuilder.DeriveParameters(command),
                        Throws.Exception.TypeOf<PostgresException>()
                        .With.Property(nameof(PostgresException.SqlState)).EqualTo("42883"));
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                        conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1 CASCADE");
                }
            }
        }

        [Test, Description("Tests if an exception is thrown if multiple functions with the specified name are in the search_path")]
        public void DeriveThrowsForMultipleFunctionNameHitsInSearchPath()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1, schema2 CASCADE; CREATE SCHEMA schema1; CREATE SCHEMA schema2;");
                try
                {
                    conn.ExecuteNonQuery(
                        @"
CREATE OR REPLACE FUNCTION schema1.redundantfunc() RETURNS int AS
$BODY$
BEGIN
    RETURN 1;
END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION schema2.redundantfunc(IN param1 INT, IN param2 INT) RETURNS int AS
$BODY$
BEGIN
RETURN param1 + param2;
END;
$BODY$
LANGUAGE 'plpgsql';

SET search_path TO schema1, schema2;
");
                    var command = new NpgsqlCommand("redundantfunc", conn) { CommandType = CommandType.StoredProcedure };
                    Assert.That(() => NpgsqlCommandBuilder.DeriveParameters(command),
                        Throws.Exception.TypeOf<PostgresException>()
                        .With.Property(nameof(PostgresException.SqlState)).EqualTo("42725"));
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                        conn.ExecuteNonQuery("DROP SCHEMA IF EXISTS schema1, schema2 CASCADE");
                }
            }
        }

        #region Set returning functions

        [Test, Description("Tests parameter derivation for a function that returns SETOF sometype")]
        public void DeriveParametersFunctionReturningSetofType()
        {
            using (var conn = OpenConnection())
            {
                TestUtil.MinimumPgVersion(conn, "9.2.0");

                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
CREATE TABLE pg_temp.foo (fooid int, foosubid int, fooname text);

INSERT INTO pg_temp.foo VALUES
(1, 1, 'Joe'),
(1, 2, 'Ed'),
(2, 1, 'Mary');

CREATE FUNCTION pg_temp.getfoo(int) RETURNS SETOF foo AS $$
    SELECT * FROM pg_temp.foo WHERE pg_temp.foo.fooid = $1 ORDER BY pg_temp.foo.foosubid;
$$ LANGUAGE SQL;
                ");

                var cmd = new NpgsqlCommand("pg_temp.getfoo", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(4));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[2].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[3].Direction, Is.EqualTo(ParameterDirection.Output));
                cmd.Parameters[0].Value = 1;
                cmd.ExecuteNonQuery();
                Assert.That(cmd.Parameters[0].Value, Is.EqualTo(1));
            }
        }

        [Test, Description("Tests parameter derivation for a function that returns TABLE")]
        public void DeriveParametersFunctionReturningTable()
        {
            using (var conn = OpenConnection())
            {
                TestUtil.MinimumPgVersion(conn, "9.2.0");

                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
CREATE TABLE pg_temp.foo (fooid int, foosubid int, fooname text);

INSERT INTO pg_temp.foo VALUES
(1, 1, 'Joe'),
(1, 2, 'Ed'),
(2, 1, 'Mary');

CREATE FUNCTION pg_temp.getfoo(int) RETURNS TABLE(fooid int, foosubid int, fooname text) AS $$
    SELECT * FROM pg_temp.foo WHERE pg_temp.foo.fooid = $1 ORDER BY pg_temp.foo.foosubid;
$$ LANGUAGE SQL;
                ");

                var cmd = new NpgsqlCommand("pg_temp.getfoo", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(4));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[2].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[3].Direction, Is.EqualTo(ParameterDirection.Output));
                cmd.Parameters[0].Value = 1;
                cmd.ExecuteNonQuery();
                Assert.That(cmd.Parameters[0].Value, Is.EqualTo(1));
            }
        }

        [Test, Description("Tests parameter derivation for a function that returns SETOF record")]
        public void DeriveParametersFunctionReturningSetofRecord()
        {
            using (var conn = OpenConnection())
            {
                TestUtil.MinimumPgVersion(conn, "9.2.0");

                // This function returns record because of the two Out (InOut & Out) parameters
                conn.ExecuteNonQuery(@"
CREATE TABLE pg_temp.foo (fooid int, foosubid int, fooname text);

INSERT INTO pg_temp.foo VALUES
(1, 1, 'Joe'),
(1, 2, 'Ed'),
(2, 1, 'Mary');

CREATE FUNCTION pg_temp.getfoo(int, OUT fooid int, OUT foosubid int, OUT fooname text) RETURNS SETOF record AS $$
    SELECT * FROM pg_temp.foo WHERE pg_temp.foo.fooid = $1 ORDER BY pg_temp.foo.foosubid;
$$ LANGUAGE SQL;
                ");

                var cmd = new NpgsqlCommand("pg_temp.getfoo", conn) { CommandType = CommandType.StoredProcedure };
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(4));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[1].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[2].Direction, Is.EqualTo(ParameterDirection.Output));
                Assert.That(cmd.Parameters[3].Direction, Is.EqualTo(ParameterDirection.Output));
                cmd.Parameters[0].Value = 1;
                cmd.ExecuteNonQuery();
                Assert.That(cmd.Parameters[0].Value, Is.EqualTo(1));
            }
        }

        #endregion

        #region CommandType.Text

        [Test, Description("Tests parameter derivation for parameterized queries (CommandType.Text)")]
        public void DeriveParametersCommandTypeTextOneParameterWithSameType()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TEMP TABLE mytable(id int, val text);");
                var cmd = new NpgsqlCommand(
                    @"INSERT INTO mytable VALUES(:x, 'some value');
                    UPDATE mytable SET val = 'changed value' WHERE id = :x;
                    SELECT val FROM mytable WHERE id = :x;",
                    conn);
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(1));
                Assert.That(cmd.Parameters[0].Direction, Is.EqualTo(ParameterDirection.Input));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("x"));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Integer));
                cmd.Parameters[0].Value = 42;
                var retVal = cmd.ExecuteScalar();
                Assert.That(retVal, Is.EqualTo("changed value"));
            }
        }

        [Test, Description("Tests parameter derivation for parameterized queries (CommandType.Text) where different types would be inferred for placeholders with the same name.")]
        public void DeriveParametersCommandTypeTextOneParameterWithDifferentTypes()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TEMP TABLE mytable(id int, val text);");
                var cmd = new NpgsqlCommand(
                    @"INSERT INTO mytable VALUES(:x, 'some value');
                    UPDATE mytable SET val = 'changed value' WHERE id = :x::double precision;
                    SELECT val FROM mytable WHERE id = :x::numeric;",
                    conn);
                var ex = Assert.Throws<NpgsqlException>(() => NpgsqlCommandBuilder.DeriveParameters(cmd));
                Assert.That(ex.Message, Is.EqualTo("The backend parser inferred different types for parameters with the same name. Please try explicit casting within your SQL statement or batch or use different placeholder names."));
            }
        }

        [Test, Description("Tests parameter derivation for parameterized queries (CommandType.Text) with multiple parameters")]
        public void DeriveParametersCommandTypeTextMultipleParameters()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TEMP TABLE mytable(id int, val text);");
                var cmd = new NpgsqlCommand(
                    @"INSERT INTO mytable VALUES(:x, 'some value');
                    UPDATE mytable SET val = 'changed value' WHERE id = @y::double precision;
                    SELECT val FROM mytable WHERE id = :z::numeric;",
                    conn);
                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(3));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("x"));
                Assert.That(cmd.Parameters[1].ParameterName, Is.EqualTo("y"));
                Assert.That(cmd.Parameters[2].ParameterName, Is.EqualTo("z"));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Integer));
                Assert.That(cmd.Parameters[1].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Double));
                Assert.That(cmd.Parameters[2].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Numeric));

                cmd.Parameters[0].Value = 42;
                cmd.Parameters[1].Value = 42d;
                cmd.Parameters[2].Value = 42;
                var retVal = cmd.ExecuteScalar();
                Assert.That(retVal, Is.EqualTo("changed value"));
            }
        }

        [Test, Description("Tests parameter derivation a parameterized query (CommandType.Text) that is already prepared.")]
        public void DeriveParametersForPreparedStatement()
        {
            const string query = "SELECT @p::integer";
            const int answer = 42;
            using (var conn = OpenConnection())
            using (var cmd = new NpgsqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@p", NpgsqlDbType.Integer, answer);
                cmd.Prepare();
                Assert.That(conn.Connector.PreparedStatementManager.NumPrepared, Is.EqualTo(1));

                var ex = Assert.Throws<NpgsqlException>(() =>
                {
                    // Derive parameters for the already prepared statement
                    NpgsqlCommandBuilder.DeriveParameters(cmd);

                });

                Assert.That(ex.Message, Is.EqualTo("Deriving parameters isn't supported for commands that are already prepared."));

                // We leave the command intact when throwing so it should still be useable
                Assert.That(cmd.Parameters.Count, Is.EqualTo(1));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("p"));
                Assert.That(conn.Connector.PreparedStatementManager.NumPrepared, Is.EqualTo(1));
                cmd.Parameters["@p"].Value = answer;
                Assert.That(cmd.ExecuteScalar(), Is.EqualTo(answer));

                conn.UnprepareAll();
            }
        }

        [Test, Description("Tests parameter derivation for array parameters in parameterized queries (CommandType.Text)")]
        public void DeriveParametersCommandTypeTextArray()
        {
            using (var conn = OpenConnection())
            {
                var cmd = new NpgsqlCommand("SELECT :a::integer[]", conn);
                var val = new[] { 7, 42 };

                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(1));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("a"));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Integer | NpgsqlDbType.Array));
                cmd.Parameters[0].Value = val;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                {
                    Assert.That(reader.Read(), Is.True);
                    Assert.That(reader.GetFieldValue<int[]>(0), Is.EqualTo(val));
                }
            }
        }

        [Test, Description("Tests parameter derivation for unmapped enum parameters in parameterized queries (CommandType.Text)")]
        public void DeriveParametersCommandTypeTextUnmappedEnum()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TYPE pg_temp.fruit AS ENUM ('Apple', 'Cherry', 'Plum')");
                conn.ReloadTypes();
                var cmd = new NpgsqlCommand("SELECT :x::fruit, :y::fruit[]", conn);
                const string val1 = "Apple";
                var val2 = new string[] { "Cherry", "Plum" };

                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(2));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("x"));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Text));
                Assert.That(cmd.Parameters[0].PostgresType, Is.InstanceOf<PostgresEnumType>());
                Assert.That(cmd.Parameters[0].PostgresType.Name, Is.EqualTo("fruit"));
                Assert.That(cmd.Parameters[0].SpecificType, Is.Null);
                Assert.That(cmd.Parameters[1].ParameterName, Is.EqualTo("y"));
                Assert.That(cmd.Parameters[1].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Text | NpgsqlDbType.Array));
                Assert.That(cmd.Parameters[1].PostgresType, Is.InstanceOf<PostgresArrayType>());
                Assert.That(cmd.Parameters[1].PostgresType.Name, Is.EqualTo("_fruit"));
                Assert.That(cmd.Parameters[1].SpecificType, Is.Null);
                cmd.Parameters[0].Value = val1;
                cmd.Parameters[1].Value = val2;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                {
                    Assert.That(reader.Read(), Is.True);
                    Assert.That(reader.GetString(0), Is.EqualTo(val1));
                    Assert.That(reader.GetFieldValue<string[]>(1), Is.EqualTo(val2));
                }
            }
        }

        enum Fruit { Apple, Cherry, Plum }

        [Test, Description("Tests parameter derivation for mapped enum parameters in parameterized queries (CommandType.Text)")]
        public void DeriveParametersCommandTypeTextMappedEnum()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TYPE pg_temp.fruit AS ENUM ('apple', 'cherry', 'plum')");
                conn.ReloadTypes();
                conn.TypeMapper.MapEnum<Fruit>("fruit");
                var cmd = new NpgsqlCommand("SELECT :x::fruit, :y::fruit[]", conn);
                const Fruit val1 = Fruit.Apple;
                var val2 = new Fruit[] { Fruit.Cherry, Fruit.Plum };

                NpgsqlCommandBuilder.DeriveParameters(cmd);
                Assert.That(cmd.Parameters, Has.Count.EqualTo(2));
                Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("x"));
                Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Enum));
                Assert.That(cmd.Parameters[0].PostgresType, Is.InstanceOf<PostgresEnumType>());
                Assert.That(cmd.Parameters[0].PostgresType.Name, Is.EqualTo("fruit"));
                Assert.That(cmd.Parameters[0].SpecificType, Is.EqualTo(typeof(Fruit)));
                Assert.That(cmd.Parameters[1].ParameterName, Is.EqualTo("y"));
                Assert.That(cmd.Parameters[1].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Enum | NpgsqlDbType.Array));
                Assert.That(cmd.Parameters[1].PostgresType, Is.InstanceOf<PostgresArrayType>());
                Assert.That(cmd.Parameters[1].PostgresType.Name, Is.EqualTo("_fruit"));
                Assert.That(cmd.Parameters[1].SpecificType, Is.EqualTo(typeof(Fruit)));
                cmd.Parameters[0].Value = val1;
                cmd.Parameters[1].Value = val2;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                {
                    Assert.That(reader.Read(), Is.True);
                    Assert.That(reader.GetFieldValue<Fruit>(0), Is.EqualTo(val1));
                    Assert.That(reader.GetFieldValue<Fruit[]>(1), Is.EqualTo(val2));
                }
            }
        }

        class SomeComposite
        {
            public int X { get; set; }
            [PgName("some_text")]
            public string SomeText { get; set; }
        }

        [Test]
        public void DeriveParametersCommandTypeTextMappedComposite()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TYPE pg_temp.deriveparameterscomposite1 AS (x int, some_text text)");
                conn.ReloadTypes();
                conn.TypeMapper.MapComposite<SomeComposite>("deriveparameterscomposite1");

                var expected1 = new SomeComposite { X = 8, SomeText = "foo" };
                var expected2 = new[] {
                    expected1,
                    new SomeComposite {X = 9, SomeText = "bar"}
                };

                using (var cmd = new NpgsqlCommand("SELECT @p1::deriveparameterscomposite1, @p2::deriveparameterscomposite1[]", conn))
                {
                    NpgsqlCommandBuilder.DeriveParameters(cmd);
                    Assert.That(cmd.Parameters, Has.Count.EqualTo(2));
                    Assert.That(cmd.Parameters[0].ParameterName, Is.EqualTo("p1"));
                    Assert.That(cmd.Parameters[0].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Composite));
                    Assert.That(cmd.Parameters[0].PostgresType, Is.InstanceOf<PostgresCompositeType>());
                    Assert.That(cmd.Parameters[0].PostgresType.Name, Is.EqualTo("deriveparameterscomposite1"));
                    Assert.That(cmd.Parameters[0].SpecificType, Is.EqualTo(typeof(SomeComposite)));
                    var p1Fields = ((PostgresCompositeType)cmd.Parameters[0].PostgresType).Fields;
                    Assert.That(p1Fields[0].PgName, Is.EqualTo("x"));
                    Assert.That(p1Fields[1].PgName, Is.EqualTo("some_text"));

                    Assert.That(cmd.Parameters[1].ParameterName, Is.EqualTo("p2"));
                    Assert.That(cmd.Parameters[1].NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Composite | NpgsqlDbType.Array));
                    Assert.That(cmd.Parameters[1].PostgresType, Is.InstanceOf<PostgresArrayType>());
                    Assert.That(cmd.Parameters[1].PostgresType.Name, Is.EqualTo("_deriveparameterscomposite1"));
                    Assert.That(cmd.Parameters[1].SpecificType, Is.EqualTo(typeof(SomeComposite)));
                    var p2Element = ((PostgresArrayType)cmd.Parameters[1].PostgresType).Element;
                    Assert.That(p2Element, Is.InstanceOf<PostgresCompositeType>());
                    Assert.That(p2Element.Name, Is.EqualTo("deriveparameterscomposite1"));
                    var p2Fields = ((PostgresCompositeType)p2Element).Fields;
                    Assert.That(p2Fields[0].PgName, Is.EqualTo("x"));
                    Assert.That(p2Fields[1].PgName, Is.EqualTo("some_text"));

                    cmd.Parameters[0].Value = expected1;
                    cmd.Parameters[1].Value = expected2;
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SingleRow))
                    {
                        Assert.That(reader.Read(), Is.True);
                        Assert.That(reader.GetFieldValue<SomeComposite>(0).SomeText, Is.EqualTo(expected1.SomeText));
                        Assert.That(reader.GetFieldValue<SomeComposite>(0).X, Is.EqualTo(expected1.X));
                        for (var i = 0; i < 2; i++)
                        {
                            Assert.That(reader.GetFieldValue<SomeComposite[]>(1)[i].SomeText, Is.EqualTo(expected2[i].SomeText));
                            Assert.That(reader.GetFieldValue<SomeComposite[]>(1)[i].X, Is.EqualTo(expected2[i].X));
                        }
                    }
                }
            }
        }

        [Test]
        public void DeriveParametersCommandTypeTextUnmappedComposite()
        {
            using (var conn = OpenConnection())
            {
                conn.ExecuteNonQuery("CREATE TYPE pg_temp.deriveparameterscomposite2 AS (x int, some_text text)");
                conn.ReloadTypes();

                using (var cmd = new NpgsqlCommand("SELECT @p1::deriveparameterscomposite2, @p2::deriveparameterscomposite2[]", conn))
                {
                    var ex = Assert.Throws<InvalidOperationException>(() => NpgsqlCommandBuilder.DeriveParameters(cmd));
                    Assert.That(ex.Message, Does.Match("^Couldn't find PostgreSQL type with OID \\d+$"));
                }
            }
        }

        #endregion
    }
}

#endif

