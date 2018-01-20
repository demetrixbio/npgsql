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
using System.Threading.Tasks;
using JetBrains.Annotations;
using Npgsql.BackendMessages;
using Npgsql.TypeHandling;

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
            if (elementType == GetElementFieldType())
                return (TArray)await ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == GetElementPsvType())
                return (TArray)await ReadPsvAsObject(buf, len, async, fieldDescription);
            throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
        }

        internal override object ReadAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription)
            => ReadAsObject(buf, len, false, fieldDescription).Result;

        internal override async ValueTask<object> ReadAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => await Read<TElement>(buf, async);

        public override ValueTask<Array> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
            => Read<TElement>(buf, async);

        protected async ValueTask<Array> Read<TElement2>(NpgsqlReadBuffer buf, bool async)
        {
            await buf.Ensure(12, async);
            var dimensions = buf.ReadInt32();
            buf.ReadInt32();        // Has nulls. Not populated by PG?
            var elementOID = buf.ReadUInt32();
            // The following should hold but fails in test CopyTests.ReadBitString
            //Debug.Assert(elementOID == ElementHandler.BackendType.OID);

            var dimLengths = new int[dimensions];

            await buf.Ensure(dimensions * 8, async);
            for (var i = 0; i < dimensions; i++)
            {
                dimLengths[i] = buf.ReadInt32();
                buf.ReadInt32(); // We don't care about the lower bounds
            }

            if (dimensions == 0)
                return new TElement2[0];   // TODO: static instance

            var result = Array.CreateInstance(typeof(TElement2), dimLengths);

            if (dimensions == 1)
            {
                var oneDimensional = (TElement2[])result;
                for (var i = 0; i < oneDimensional.Length; i++)
                    oneDimensional[i] = await ElementHandler.ReadWithLength<TElement2>(buf, async);
                return oneDimensional;
            }

            // Multidimensional
            var indices = new int[dimensions];
            while (true)
            {
                var element = await ElementHandler.ReadWithLength<TElement2>(buf, async);
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

        private static readonly TElement[] EmptyOneDimensionalArray = new TElement[0];

        protected virtual Array InitArray(bool nullable, int[] lengths, int[] lowerBounds = null)
        {
            if (lengths.Length == 0 && (lowerBounds == null || lowerBounds[0] == 0))
                return EmptyOneDimensionalArray;
            if (lowerBounds == null)
                return Array.CreateInstance(typeof(TElement), lengths);

            return Array.CreateInstance(typeof(TElement), lengths, lowerBounds);
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
                    return ValidateAndGetLengthGeneric(asGeneric, ref lengthCache, parameter);
                case IList asNonGeneric:
                    return ValidateAndGetLengthNonGeneric(asNonGeneric, ref lengthCache, parameter);
                default:
                    throw new InvalidCastException($"Can't write type {value.GetType()} as an array of {typeof(TElement2)}");
            }
        }

        /// <summary>
        /// Handle single-dimensional arrays and generic IList
        /// </summary>
        int ValidateAndGetLengthGeneric<TElement2>(IList<TElement2> value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
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
                value.Sum(e => 4 + GetSingleElementLength(e, ref lengthCache2, parameter));
            lengthCache = lengthCache2;
            return lengthCache.Lengths[pos] = len;
        }

        int GetSingleElementLength<T2>([CanBeNull] T2 element, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
        {
            if (element == null || typeof(T2) == typeof(DBNull))
                return 0;
            try
            {
                return ElementHandler.ValidateAndGetLength(element, ref lengthCache, parameter);
            }
            catch (Exception e)
            {
                throw new Exception("While trying to write an array, one of its elements failed validation. You may be trying to mix types in a non-generic IList, or to write a jagged array.", e);
            }
        }

        /// <summary>
        /// Take care of multi-dimensional arrays and non-generic IList, we have no choice but to box/unbox
        /// </summary>
        int ValidateAndGetLengthNonGeneric(IList value, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
        {
            var asMultidimensional = value as Array;
            var dimensions = asMultidimensional?.Rank ?? 1;
            var lengths = new int[dimensions];
            Array convertedArray;

            // Leave empty slot for the entire array length, and go ahead an populate the element slots
            var pos = lengthCache.Position;
            lengthCache.Set(0);
            var lengthCache2 = lengthCache;
            var len =
                4 +       // dimensions
                4 +       // has_nulls (unused)
                4 +       // type OID
                dimensions * 8;  // number of dimensions * (length + lower bound)

            var hasConvertedValue = false;
            if(dimensions == 1)
            {
                var oneDimLength = value.Count;
                lengths[0] = oneDimLength;
                convertedArray = InitArray(true, lengths);
                for(var i = oneDimLength - 1; i >= 0; i--)
                {
                    len += 4 + GetSingleElementObjectLength(value[i], ref lengthCache2, parameter);
                    if (parameter.ConvertedValue != null)
                    {
                        convertedArray.SetValue(parameter.ConvertedValue, i);
                        hasConvertedValue = true;
                    }
                }
            }
            else
            {
                var lowerBounds = new int[dimensions];
                for(var i = dimensions - 1; i >= 0; i--)
                {
                    lengths[i] = asMultidimensional.GetLength(i);
                    lowerBounds[i] = asMultidimensional.GetLowerBound(i);
                }
                convertedArray = InitArray(true, lengths, lowerBounds);
                var indices = new int[dimensions];
                var currentDimIndex = 0;
                hasConvertedValue = GetSingleElementObjectLengthsRecursive(convertedArray, asMultidimensional, ref currentDimIndex, indices, ref lengthCache2, parameter, ref len);
            }
            if(hasConvertedValue)
                parameter.ConvertedValue = convertedArray;

            lengthCache = lengthCache2;
            lengthCache.Lengths[pos] = len;
            return len;
        }

        private bool GetSingleElementObjectLengthsRecursive(Array target, Array array, ref int currentDimIndex, int[] indices, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter, ref int length)
        {
            var hasConvertedValue = false;
            for (var i = array.GetLowerBound(currentDimIndex); i <= array.GetUpperBound(currentDimIndex); i++)
            {
                indices[currentDimIndex] = i;
                if (currentDimIndex < array.Rank - 1)
                {
                    currentDimIndex++;
                    var ret = GetSingleElementObjectLengthsRecursive(target, array, ref currentDimIndex, indices, ref lengthCache, parameter, ref length);
                    if (!hasConvertedValue)
                        hasConvertedValue = ret;
                }
                else
                {
                    length += 4 + GetSingleElementObjectLength(array.GetValue(indices), ref lengthCache, parameter);
                    if (parameter.ConvertedValue != null)
                    {
                        target.SetValue(parameter.ConvertedValue, indices);
                        hasConvertedValue = true;
                    }
                }
            }
            currentDimIndex--;
            return hasConvertedValue;
        }

        int GetSingleElementObjectLength([CanBeNull] object element, ref NpgsqlLengthCache lengthCache, NpgsqlParameter parameter)
        {
            if (element == null || element is DBNull)
                return 0;
            try
            {
                return ElementHandler.ValidateObjectAndGetLength(element, ref lengthCache, parameter);
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
        {
            if (value == null || value is DBNull)
                return WriteWithLengthInternal<DBNull>(null, buf, lengthCache, parameter, async);
            if (parameter.ConvertedValue != null)
                return WriteWithLengthInternal(parameter.ConvertedValue, buf, lengthCache, parameter, async);

            return WriteWithLengthInternal(value, buf, lengthCache, parameter, async);
        }

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

    class StringArrayHandler : ArrayHandler<string>
    {
        private Lazy<ArrayHandler<char[]>> charArrayHandler;

        public StringArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler, int lowerBound) : base(elementHandler, lowerBound)
        {
            charArrayHandler = new Lazy<ArrayHandler<char[]>>(()=> { return new ArrayHandler<char[]>(ElementHandler); });
        }

        public StringArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler) : this(elementHandler, 1) { }

        protected internal override async ValueTask<TArray> Read<TArray>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var t = typeof(TArray);
            if (!t.IsArray)
                throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");

            // Getting an array

            // We need to treat this as an actual array type, these need special treatment because of
            // typing/generics reasons (there is no way to express "array of X" with generics
            var elementType = t.GetElementType();
            var elementFieldType = GetElementFieldType();
            if (elementType == elementFieldType)
                return (TArray)await ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == GetElementPsvType())
                return (TArray)await ReadPsvAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(char[]))
                return (TArray)(object)await charArrayHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
        }
    }
    class NumericArrayHandler<TDefaultElement> : ArrayHandler<TDefaultElement>
        where TDefaultElement : struct
    {
        private Lazy<ArrayHandler<byte>> byteHandler;
        private Lazy<ArrayHandler<sbyte>> sbyteHandler;
        private Lazy<ArrayHandler<short>> shortHandler;
        private Lazy<ArrayHandler<int>> intHandler;
        private Lazy<ArrayHandler<long>> longHandler;
        private Lazy<ArrayHandler<float>> floatHandler;
        private Lazy<ArrayHandler<double>> doubleHandler;
        private Lazy<ArrayHandler<decimal>> decimalHandler;
        private Lazy<ArrayHandler<string>> stringHandler;

        public NumericArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler, int lowerBound) : base(elementHandler, lowerBound)
        {
            byteHandler = new Lazy<ArrayHandler<byte>>(InitializeArrayHandler<byte>);
            sbyteHandler = new Lazy<ArrayHandler<sbyte>>(InitializeArrayHandler<sbyte>);
            shortHandler = new Lazy<ArrayHandler<short>>(InitializeArrayHandler<short>);
            intHandler = new Lazy<ArrayHandler<int>>(InitializeArrayHandler<int>);
            longHandler = new Lazy<ArrayHandler<long>>(InitializeArrayHandler<long>);
            floatHandler = new Lazy<ArrayHandler<float>>(InitializeArrayHandler<float>);
            doubleHandler = new Lazy<ArrayHandler<double>>(InitializeArrayHandler<double>);
            decimalHandler = new Lazy<ArrayHandler<decimal>>(InitializeArrayHandler<decimal>);
            stringHandler = new Lazy<ArrayHandler<string>>(InitializeArrayHandler<string>);
        }

        public NumericArrayHandler([CanBeNull] NpgsqlTypeHandler elementHandler) : this(elementHandler, 1) {}

        private ArrayHandler<T> InitializeArrayHandler<T>()
        {
            return new ArrayHandler<T>(ElementHandler);
        }

        protected internal override async ValueTask<TArray> Read<TArray>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription = null)
        {
            var t = typeof(TArray);
            if (!t.IsArray)
                throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
            var elementType = t.GetElementType();
            var elementFieldType = GetElementFieldType();

            if (elementType == elementFieldType)
                return (TArray)await ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(byte))
                return (TArray)(object)await byteHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(sbyte))
                return (TArray)(object)await sbyteHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(short))
                return (TArray)(object)await shortHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(int))
                return (TArray)(object)await intHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(long))
                return (TArray)(object)await longHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(float))
                return (TArray)(object)await floatHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(double))
                return (TArray)(object)await doubleHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(decimal))
                return (TArray)(object)await decimalHandler.Value.ReadAsObject(buf, len, async, fieldDescription);
            if (elementType == typeof(string))
                return (TArray)(object)await stringHandler.Value.ReadAsObject(buf, len, async, fieldDescription);

            throw new InvalidCastException($"Can't cast database type {PgDisplayName} to {typeof(TArray).Name}");
        }
    }
    
    /// <remarks>
    /// http://www.postgresql.org/docs/current/static/arrays.html
    /// </remarks>
    /// <typeparam name="TElement">The .NET type contained as an element within this array</typeparam>
    /// <typeparam name="TElementPsv">The .NET provider-specific type contained as an element within this array</typeparam>
    class ArrayHandlerWithPsv<TElement, TElementPsv> : ArrayHandler<TElement>
    {
        /// <summary>
        /// The provider-specific type of the elements contained within this array,
        /// </summary>
        /// <param name="fieldDescription"></param>
        internal override Type GetElementPsvType(FieldDescription fieldDescription)
            => typeof(TElementPsv);

        internal override object ReadPsvAsObject(NpgsqlReadBuffer buf, int len, FieldDescription fieldDescription)
            => ReadPsvAsObject(buf, len, false, fieldDescription).Result;

        internal override async ValueTask<object> ReadPsvAsObject(NpgsqlReadBuffer buf, int len, bool async, FieldDescription fieldDescription)
            => await Read<TElementPsv>(buf, async);

        public ArrayHandlerWithPsv(NpgsqlTypeHandler elementHandler)
            : base(elementHandler) {}
    }
}
