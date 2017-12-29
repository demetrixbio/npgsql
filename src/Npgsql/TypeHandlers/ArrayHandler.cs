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
using System.Diagnostics;
using System.Linq;
#if NETSTANDARD1_3
using System.Reflection;
#endif
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;
using Npgsql.Util;

namespace Npgsql.TypeHandlers
{
    abstract class ArrayHandler : NpgsqlTypeHandler<Array>
    {
        internal abstract Type GetElementFieldType(FieldDescription fieldDescription = null);
        internal abstract Type GetElementPsvType(FieldDescription fieldDescription = null);
    }

#pragma warning disable CA1061 // Do not hide base class methods
#pragma warning disable CA1801 // Review unused parameters
    /// <summary>
    /// Base class for all type handlers which handle PostgreSQL arrays.
    /// </summary>
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/arrays.html
    /// </remarks>
    class ArrayHandler<TElement> : ArrayHandler
    {
        /// <summary>
        /// The lower bound value sent to the backend when writing arrays. Normally 1 (the PG default) but
        /// is 0 for OIDVector.
        /// </summary>
        protected int LowerBound { get; set; }

        internal override Type GetFieldType(FieldDescription fieldDescription = null) => typeof(Array);
        internal override Type GetProviderSpecificFieldType(FieldDescription fieldDescription = null) => typeof(Array);

        /// <summary>
        /// The type of the elements contained within this array
        /// </summary>
        /// <param name="fieldDescription"></param>
        internal override Type GetElementFieldType(FieldDescription fieldDescription = null) => typeof(TElement);

        /// <summary>
        /// The provider-specific type of the elements contained within this array,
        /// </summary>
        /// <param name="fieldDescription"></param>
        internal override Type GetElementPsvType(FieldDescription fieldDescription = null) => typeof(TElement);

        /// <summary>
        /// The type handler for the element that this array type holds
        /// </summary>
        protected internal NpgsqlTypeHandler ElementHandler { get; protected set; }

        public ArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler, int lowerBound)
        {
            LowerBound = lowerBound;
            ElementHandler = elementHandler;
        }

        public ArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler) : this(elementHandler, 1) {}

        #region Read

        // Note that unlike most type handlers, we have to override Read<T2> and not Read, since we
        // must do array-specific checking etc.
        protected internal override async ValueTask<TArray> Read<TArray>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            // TODO: Throw SafeReadExceptions
            var t = typeof(TArray);
            if (!t.IsArray)
                throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");

            // Getting an array

            // We need to treat this as an actual array type, these need special treatment because of
            // typing/generics reasons (there is no way to express "array of X" with generics
            var elementType = t.GetElementType();
            var elementFieldType = GetElementFieldType();
            if (elementType == elementFieldType)
                return (TArray)(object)await Read(buf, async, false);
            if (Nullable.GetUnderlyingType(elementType) == elementFieldType)
                return (TArray)(object)await Read(buf, async, true);
            if (elementType == GetElementPsvType())
                return (TArray)await ReadPsvAsObject(buf, len, async, fieldDescription);
            throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
        }

        internal override object ReadAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription)
            => ReadAsObject(buf, len, false, fieldDescription).Result;

        internal override async ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => await Read(buf, async);

        public override ValueTask<Array> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => Read(buf, async);

        internal async ValueTask<Array> Read(NpgsqlReadBuffer buf, bool async, bool? forcedNullability = null)
        {
            await buf.Ensure(12, async);
            var dimensions = buf.ReadInt32();
            var containsNulls = Convert.ToBoolean(buf.ReadInt32());
            if (forcedNullability.HasValue)
            {

                // This explicitly breaks compatibility with Npgsql Versions < 3.3
                // because it prevents returning default(T) for value types
                // Removing this exception would restore the previous (broken) functionality
                if (TypeHelper.IsValueType(typeof(TElement)) && containsNulls && !forcedNullability.Value)
                    throw new InvalidOperationException(
                        $"Can't read a non-nullable array of '{typeof(TElement).Name}' if the database array field contains null values.");

                containsNulls = forcedNullability.Value;
            }

            var elementOID = buf.ReadUInt32();
            // TODO: Check if the following is stil true after the modifications to allow nullable arrays
            // The following should hold but fails in test CopyTests.ReadBitString
            //Debug.Assert(elementOID == ElementHandler.BackendType.OID);

            var dimLengths = new int[dimensions];

            await buf.Ensure(dimensions * 8, async);
            for (var i = 0; i < dimensions; i++)
            {
                dimLengths[i] = buf.ReadInt32();
                buf.ReadInt32(); // We don't care about the lower bounds
            }

            var result = InitArray(containsNulls, dimLengths);

            if (dimensions == 0)
                return result;

            if (dimensions == 1)
                return await FillOneDimensional(result, containsNulls, buf, async);

            // Multidimensional
            var indices = new int[dimensions];
            while (true)
            {
                var element = await ReadElementAsObject(containsNulls, buf, async);
                result.SetValue(element, indices);

                // TODO: Overly complicated/inefficient...
                indices[dimensions - 1]++;
                for (var dim = dimensions - 1; dim >= 0; dim--)
                {
                    if (indices[dim] <= result.GetUpperBound(dim))
                        continue;

                    if (dim == 0)
                        return result;

                    for (var j = dim; j < dimensions; j++)
                        indices[j] = result.GetLowerBound(j);
                    indices[dim - 1]++;
                }
            }
        }


        protected virtual async ValueTask<Array> FillOneDimensional(Array result, bool nullable, NpgsqlReadBuffer buf, bool async)
        {
            var oneDimensional = (TElement[])result;
            for (var i = 0; i < oneDimensional.Length; i++)
                oneDimensional[i] = await ElementHandler.ReadWithLength<TElement>(buf, async);
            return oneDimensional;
        }

        protected virtual async ValueTask<object> ReadElementAsObject(bool nullable, NpgsqlReadBuffer buf, bool async)
        {
            return (object)await ElementHandler.ReadWithLength<TElement>(buf, async);
        }

        protected virtual Array InitArray(bool nullable, int[] lengths)
        {
            if(lengths.Length == 0)
                return new TElement[0];   // TODO: static instance

            return Array.CreateInstance(typeof(TElement), lengths);
        }
        #endregion

        #region Write

        public override int ValidateAndGetLength<T2>(T2 value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => ValidateAndGetLength<TElement>(value, ref lengthCache, parameter);

        protected internal override int ValidateObjectAndGetLength(object value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter = null)
            => ValidateAndGetLength<TElement>(value, ref lengthCache, parameter);

        // We're required to override this but it will never be called
        public override int ValidateAndGetLength(Array value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
            => throw new NotSupportedException();

        public int ValidateAndGetLength<TElement2>(object value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter = null)
        {
            if (lengthCache == null)
                lengthCache = new NpgsqlLengthCache(1);
            if (lengthCache.IsPopulated)
                return lengthCache.Get();

            switch (value)
            {
                case IList<TElement2> asGeneric:
                    return ValidateAndGetLengthGeneric(asGeneric, ref lengthCache);
                case IList asNonGeneric:
                    return ValidateAndGetLengthNonGeneric(asNonGeneric, ref lengthCache);
                default:
                    throw new InvalidCastException($"Can't write type {value.GetType()} as an array of {typeof(TElement2)}");
            }
        }

        /// <summary>
        /// Handle single-dimensional arrays and generic IList
        /// </summary>
        int ValidateAndGetLengthGeneric<TElement2>(IList<TElement2> value, ref NpgsqlLengthCache lengthCache)
        {
            // Leave empty slot for the entire array length, and go ahead an populate the element slots
            var pos = lengthCache.Position;
            lengthCache.Set(0);
            var lengthCache2 = lengthCache;
            var len =
                4 +       // dimensions
                4 +       // has_nulls (unused)
                4 +       // type OID
                1 * 8 +   // number of dimensions (1) * (length + lower bound)
                value.Sum(e => 4 + GetSingleElementLength(e, ref lengthCache2));
            lengthCache = lengthCache2;
            return lengthCache.Lengths[pos] = len;
        }

        int GetSingleElementLength<T2>([CanBeNull] T2 element, ref NpgsqlLengthCache lengthCache)
        {
            if (element == null || typeof(T2) == typeof(DBNull))
                return 0;
            try
            {
                return ElementHandler.ValidateAndGetLength(element, ref lengthCache, null);
            }
            catch (Exception e)
            {
                throw new Exception("While trying to write an array, one of its elements failed validation. You may be trying to mix types in a non-generic IList, or to write a jagged array.", e);
            }
        }

        /// <summary>
        /// Take care of multi-dimensional arrays and non-generic IList, we have no choice but to box/unbox
        /// </summary>
        int ValidateAndGetLengthNonGeneric(IList value, ref NpgsqlLengthCache lengthCache)
        {
            var asMultidimensional = value as Array;
            var dimensions = asMultidimensional?.Rank ?? 1;

            // Leave empty slot for the entire array length, and go ahead an populate the element slots
            var pos = lengthCache.Position;
            lengthCache.Set(0);
            var lengthCache2 = lengthCache;
            var len =
                4 +       // dimensions
                4 +       // has_nulls (unused)
                4 +       // type OID
                dimensions * 8 +  // number of dimensions * (length + lower bound)
                value.Cast<object>().Sum(element => 4 + GetSingleElementObjectLength(element, ref lengthCache2));
            lengthCache = lengthCache2;
            lengthCache.Lengths[pos] = len;
            return len;
        }

        int GetSingleElementObjectLength([CanBeNull] object element, ref NpgsqlLengthCache lengthCache)
        {
            if (element == null || element is DBNull)
                return 0;
            try
            {
                return ElementHandler.ValidateObjectAndGetLength(element, ref lengthCache, null);
            }
            catch (Exception e)
            {
                throw new Exception("While trying to write an array, one of its elements failed validation. You may be trying to mix types in a non-generic IList, or to write a jagged array.", e);
            }
        }

        protected override Task WriteWithLength<T2>(T2 value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
        {
            buf.WriteInt32(ValidateAndGetLength<TElement>(value, ref lengthCache, parameter));
            return Write(value, buf, lengthCache, async);
        }

        public override Task Write(Array value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => throw new NotSupportedException("ArrayHandler overrides Write<T2> so we shouldn't get here");

        // The default WriteObjectWithLength casts the type handler to INpgsqlTypeHandler<T>, but that's not sufficient for
        // us (need to handle many types of T, e.g. int[], int[,]...)
        protected internal override Task WriteObjectWithLength(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, bool async)
            => value == null || value is DBNull
                ? WriteWithLengthInternal<DBNull>(null, buf, lengthCache, parameter, async)
                : WriteWithLengthInternal(value, buf, lengthCache, parameter, async);

        // TODO: Implement WriteVector which writes arrays without boxing... (accept IList<T2>)
        async Task Write(object value, NpgsqlWriteBuffer buf, NpgsqlLengthCache lengthCache, bool async)
        {
            var asArray = value as Array;
            var dimensions = asArray?.Rank ?? 1;
            var writeValue = (IList)value;

            var len =
                4 +               // ndim
                4 +               // has_nulls
                4 +               // element_oid
                dimensions * 8;   // dim (4) + lBound (4)

            if (buf.WriteSpaceLeft < len)
            {
                await buf.Flush(async);
                Debug.Assert(buf.WriteSpaceLeft >= len, "Buffer too small for header");
            }

            buf.WriteInt32(dimensions);
            buf.WriteInt32(1);  // HasNulls=1. Not actually used by the backend.
            buf.WriteUInt32(ElementHandler.PostgresType.OID);
            if (asArray != null)
            {
                for (var i = 0; i < dimensions; i++)
                {
                    buf.WriteInt32(asArray.GetLength(i));
                    buf.WriteInt32(LowerBound);  // We don't map .NET lower bounds to PG
                }
            }
            else
            {
                buf.WriteInt32(writeValue.Count);
                buf.WriteInt32(LowerBound);  // We don't map .NET lower bounds to PG
            }

            foreach (var element in writeValue)
                await ElementHandler.WriteObjectWithLength(element, buf, lengthCache, null, async);
        }

        #endregion
    }
#pragma warning restore CA1061 // Do not hide base class methods
#pragma warning restore CA1801 // Review unused parameters

    class ValueTypeArrayHandler<TElement> : ArrayHandler<TElement>
        where TElement : struct
    {
        public ValueTypeArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler, int lowerBound) : base(elementHandler, lowerBound) { }

        public ValueTypeArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler) : base(elementHandler) { }

        protected override Array InitArray(bool nullable, int[] lengths)
        {
            if (nullable)
            {
                if (lengths.Length == 0)
                    return new TElement?[0];   // TODO: static instance

                return Array.CreateInstance(typeof(TElement?), lengths);
            }
            return base.InitArray(nullable, lengths);
        }

        protected override async ValueTask<Array> FillOneDimensional(Array result, bool nullable, NpgsqlReadBuffer buf, bool async)
        {
            if (nullable)
            {
                var oneDimensional = (TElement?[])result;
                for (var i = 0; i < oneDimensional.Length; i++)
                    oneDimensional[i] = await ElementHandler.ReadNullableWithLength<TElement>(buf, async);
                return oneDimensional;
            }
            return await base.FillOneDimensional(result, nullable, buf, async);
        }

        protected override async ValueTask<object> ReadElementAsObject(bool nullable, NpgsqlReadBuffer buf, bool async)
        {
            if (nullable)
                return await ElementHandler.ReadNullableWithLength<TElement>(buf, async);

            return await base.ReadElementAsObject(nullable, buf, async);
        }
    }

    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/arrays.html
    /// </remarks>
    /// <typeparam name="TElement">The .NET type contained as an element within this array</typeparam>
    /// <typeparam name="TElementPsv">The .NET provider-specific type contained as an element within this array</typeparam>
    class ArrayHandlerWithPsv<TElement, TElementPsv> : ArrayHandler<TElement>
    {
        NpgsqlTypeHandler psvHandler;

        /// <summary>
        /// The provider-specific type of the elements contained within this array,
        /// </summary>
        /// <param name="fieldDescription"></param>
        internal override Type GetElementPsvType(FieldDescription fieldDescription)
            => typeof(TElementPsv);

        internal override object ReadPsvAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription)
            => ReadPsvAsObject(buf, len, false, fieldDescription).Result;

        internal override async ValueTask<object> ReadPsvAsObject(NpgsqlReadBuffer buf, int len, bool async,
            FieldDescription fieldDescription)
            => await psvHandler.Read<TElementPsv[]>(buf, len, async, fieldDescription);

        public ArrayHandlerWithPsv(NpgsqlTypeHandler elementHandler)
            : base(elementHandler)
        {
            psvHandler = new ArrayHandler<TElementPsv>(elementHandler);
        }
    }
}
