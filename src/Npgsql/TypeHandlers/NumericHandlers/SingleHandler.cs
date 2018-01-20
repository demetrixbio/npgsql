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

using Npgsql.BackendMessages;
using NpgsqlTypes;
using System.Data;
using JetBrains.Annotations;
using Npgsql.TypeHandling;
using Npgsql.TypeMapping;
using System;
using Npgsql.PostgresTypes;

namespace Npgsql.TypeHandlers.NumericHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/datatype-numeric.html
    /// </remarks>
    [TypeMapping("float4", NpgsqlDbType.Real, DbType.Single, typeof(float))]
    class SingleHandler : NpgsqlSimpleTypeHandler<float>, INpgsqlSimpleTypeHandler<double>, INpgsqlSimpleTypeHandler<IConvertible>
    {
        private const int Float4Length = 4;

        #region Read

        public override float Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription = null)
            => buf.ReadSingle();

        double INpgsqlSimpleTypeHandler<double>.Read(NpgsqlReadBuffer buf, int len, [CanBeNull] FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        IConvertible INpgsqlSimpleTypeHandler<IConvertible>.Read(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription)
            => Read(buf, len, fieldDescription);

        #endregion Read

        #region Write

        public int ValidateAndGetLength(double value, NpgsqlParameter parameter)
            => Float4Length;

        public override int ValidateAndGetLength(float value, NpgsqlParameter parameter)
            => Float4Length;

        public int ValidateAndGetLength(IConvertible value, [CanBeNull] NpgsqlParameter parameter)
        {
            if (parameter == null)
                throw CreateConversionButNoParamException(value.GetType());

            var converted = Convert.ToSingle(value);
            parameter.ConvertedValue = converted;
            return Float4Length;
        }

        public void Write(double value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteSingle((float)value);

        public override void Write(float value, NpgsqlWriteBuffer buf, NpgsqlParameter parameter)
            => buf.WriteSingle(value);

        public void Write(IConvertible value, NpgsqlWriteBuffer buf, [CanBeNull] NpgsqlParameter parameter)
            => buf.WriteSingle((float)parameter.ConvertedValue);

        #endregion Write

        internal override ArrayHandler CreateArrayHandler(PostgresType arrayBackendType)
            => new NumericArrayHandler<float>(this) { PostgresType = arrayBackendType };
    }
}
