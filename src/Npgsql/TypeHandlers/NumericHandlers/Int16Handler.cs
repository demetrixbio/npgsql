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
using Npgsql.BackendMessages;
using NpgsqlTypes;
using System.Data;
using System.Diagnostics;
using JetBrains.Annotations;
using Npgsql.PostgresTypes;
using Npgsql.TypeHandling;
using Npgsql.TypeMapping;
using System.Threading.Tasks;

namespace Npgsql.TypeHandlers.NumericHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/datatype-numeric.html
    /// </remarks>
    [TypeMapping("int2", NpgsqlDbType.Smallint, new[] { DbType.Int16, DbType.Byte, DbType.SByte }, new[] { typeof(short), typeof(byte), typeof(sbyte), typeof(short?), typeof(byte?), typeof(sbyte?) }, DbType.Int16)]
    class Int16Handler : NpgsqlSimpleTypeHandler<short>,
        INpgsqlSimpleTypeHandler<byte>, INpgsqlSimpleTypeHandler<sbyte>, INpgsqlSimpleTypeHandler<int>, INpgsqlSimpleTypeHandler<long>,
        INpgsqlSimpleTypeHandler<float>, INpgsqlSimpleTypeHandler<double>, INpgsqlSimpleTypeHandler<decimal>,
        INpgsqlSimpleTypeHandler<string>
    {
        #region Read

        public override short Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => buf.ReadInt16();

        byte INpgsqlSimpleTypeHandler<byte>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => (byte)Read(buf, len, fieldDescription);

        sbyte INpgsqlSimpleTypeHandler<sbyte>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => (sbyte)Read(buf, len, fieldDescription);

        int INpgsqlSimpleTypeHandler<int>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        long INpgsqlSimpleTypeHandler<long>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        float INpgsqlSimpleTypeHandler<float>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        double INpgsqlSimpleTypeHandler<double>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        decimal INpgsqlSimpleTypeHandler<decimal>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        string INpgsqlSimpleTypeHandler<string>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription).ToString();

        #endregion Read

        #region Write

        public override int ValidateAndGetLength(short value, NpgsqlParameter parameter) => 2;
        public int ValidateAndGetLength(int value, NpgsqlParameter parameter)            => 2;
        public int ValidateAndGetLength(long value, NpgsqlParameter parameter)           => 2;
        public int ValidateAndGetLength(byte value, NpgsqlParameter parameter)           => 2;
        public int ValidateAndGetLength(sbyte value, NpgsqlParameter parameter)          => 2;
        public int ValidateAndGetLength(float value, NpgsqlParameter parameter)          => 2;
        public int ValidateAndGetLength(double value, NpgsqlParameter parameter)         => 2;
        public int ValidateAndGetLength(decimal value, NpgsqlParameter parameter)        => 2;

        public int ValidateAndGetLength(string value, NpgsqlParameter parameter)
        {
            var converted = Convert.ToInt16(value);
            if (parameter == null)
                throw CreateConversionButNoParamException(value.GetType());
            parameter.ConvertedValue = converted;
            return 2;
        }

        public override void Write(short value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16(value);
        public void Write(int value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16((short)value);
        public void Write(long value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16((short)value);
        public void Write(byte value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16(value);
        public void Write(sbyte value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16(value);
        public void Write(decimal value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16((short)value);
        public void Write(double value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16((short)value);
        public void Write(float value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteInt16((short)value);

        public void Write(string value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
        {
            Debug.Assert(parameter != null);
            buf.WriteInt16((short)parameter.ConvertedValue);
        }

        #endregion Write

        internal override ArrayHandler CreateArrayHandler(PostgresType arrayBackendType)
            => new Int16ArrayHandler(this) { PostgresType = arrayBackendType };
    }

    class Int16ArrayHandler : ValueTypeArrayHandler<short>
    {
        ValueTypeArrayHandler<byte> byteHandler;
        ValueTypeArrayHandler<sbyte> sbyteHandler;

        public Int16ArrayHandler(Int16Handler elementHandler)
            : base(elementHandler)
        {
            byteHandler = new ValueTypeArrayHandler<byte>(elementHandler);
            sbyteHandler = new ValueTypeArrayHandler<sbyte>(elementHandler);
        }

        protected internal override async ValueTask<TArray> Read<TArray>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var t = typeof(TArray);
            if (!t.IsArray)
                throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
            var elementType = t.GetElementType();
            var elementFieldType = GetElementFieldType();

            if (elementType == elementFieldType)
                return (TArray)(object)await Read(buf, async, false);
            if (Nullable.GetUnderlyingType(elementType) == elementFieldType)
                return (TArray)(object)await Read(buf, async, true);
            if (elementType == typeof(byte))
                return (TArray)(object)await byteHandler.Read(buf, async, false);
            if (Nullable.GetUnderlyingType(elementType) == typeof(byte))
                return (TArray)(object)await byteHandler.Read(buf, async, true);
            if (elementType == typeof(sbyte))
                return (TArray)(object)await sbyteHandler.Read(buf, async, false);
            if (Nullable.GetUnderlyingType(elementType) == typeof(sbyte))
                return (TArray)(object)await sbyteHandler.Read(buf, async, true);
            throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
        }

        internal override ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
        {
            return base.ReadAsObject(buf, len, async, fieldDescription);
        }

    }
}
