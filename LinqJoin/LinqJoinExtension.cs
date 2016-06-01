using System;
using System.Collections.Generic;
using System.Linq;

namespace LinqJoin
{
    /// <summary>
    /// Original: http://www.codeproject.com/Articles/488643/LinQ-Extended-Joins
    /// </summary>
    public static class LinqJoinExtension
    {
        public static IEnumerable<TResult> LeftJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            return from s in source
                join i in inner
                    on pk(s) equals fk(i) into joinData
                from left in joinData.DefaultIfEmpty()
                select result(s, left);
        }

        public static IEnumerable<TResult> RightJoin<TSource, TInner, TKey, TResult>(this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            return from i in inner
                join s in source
                    on fk(i) equals pk(s) into joinData
                from right in joinData.DefaultIfEmpty()
                select result(right, i);
        }

        public static IEnumerable<TResult> FullOuterJoinJoin<TSource, TInner, TKey, TResult>(
            this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            var sources = source as TSource[] ?? source.ToArray();
            var inners = inner as TInner[] ?? inner.ToArray();
            var left = sources.LeftJoin(inners, pk, fk, result).ToList();
            var right = sources.RightJoin(inners, pk, fk, result).ToList();

            return left.Union(right);
        }

        public static IEnumerable<TResult> LeftExcludingJoin<TSource, TInner, TKey, TResult>(
            this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            return from s in source
                join i in inner
                    on pk(s) equals fk(i) into joinData
                from left in joinData.DefaultIfEmpty()
                where left == null
                select result(s, left);
        }

        public static IEnumerable<TResult> RightExcludingJoin<TSource, TInner, TKey, TResult>(
            this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            return from i in inner
                join s in source
                    on fk(i) equals pk(s) into joinData
                from right in joinData.DefaultIfEmpty()
                where right == null
                select result(right, i);
        }

        public static IEnumerable<TResult> FulltExcludingJoin<TSource, TInner, TKey, TResult>(
            this IEnumerable<TSource> source,
            IEnumerable<TInner> inner,
            Func<TSource, TKey> pk,
            Func<TInner, TKey> fk,
            Func<TSource, TInner, TResult> result)
        {
            var sources = source as TSource[] ?? source.ToArray();
            var inners = inner as TInner[] ?? inner.ToArray();
            var left = sources.LeftExcludingJoin(inners, pk, fk, result).ToList();
            var right = sources.RightExcludingJoin(inners, pk, fk, result).ToList();

            return left.Union(right);
        }
    }
}