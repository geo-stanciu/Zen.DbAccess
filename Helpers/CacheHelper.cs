using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Zen.DbAccess.Helpers;

public static class CacheHelper
{
    private static readonly ConcurrentDictionary<string, object?> _cache = new ConcurrentDictionary<string, object?>();
    private static readonly ConcurrentDictionary<string, DateTime> _cacheLastUsed = new ConcurrentDictionary<string, DateTime>();

    public static void Clear()
    {
        _cache.Clear();
        _cacheLastUsed.Clear();
    }

    public static void Remove(string key)
    {
        _cache.TryRemove(key, out _);
        _cacheLastUsed.TryRemove(key, out _);
    }

    public static bool TryAdd<T>(string key, T value)
    {
        _cacheLastUsed[key] = DateTime.UtcNow;
        var added = _cache.TryAdd(key, value);

        ClearOldestUsedEntriesIfTooMany();

        return added;
    }

    public static bool TryGetValue<T>(string key, out T? value)
    {
        if (_cache.TryGetValue(key, out var cachedValue) && cachedValue != null && cachedValue is T typedValue)
        {
            _cacheLastUsed[key] = DateTime.UtcNow;
            value = typedValue;
            return true;
        }

        value = default;
        return false;
    }

    public static T GetOrAdd<T>(string key, Func<T> valueFactory)
    {
        if (_cache.TryGetValue(key, out var cachedValue) && cachedValue != null && cachedValue is T typedValue)
        {
            _cacheLastUsed[key] = DateTime.UtcNow;
            return typedValue;
        }

        var newValue = valueFactory();

        if (newValue == null)
            return newValue;

        _cacheLastUsed[key] = DateTime.UtcNow;
        _cache.TryAdd(key, newValue);

        ClearOldestUsedEntriesIfTooMany();

        return newValue;
    }

    private static void ClearOldestUsedEntriesIfTooMany()
    {
        if (_cacheLastUsed.Count <= Constants.DbAccessConstants.MaxQueryPropertiesCache)
            return;

        var keysToRemove = _cacheLastUsed
            .OrderBy(kvp => kvp.Value)
            .Take(Constants.DbAccessConstants.MaxQueryPropertiesCacheCleanupCount)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in keysToRemove)
        {
            _cache.TryRemove(key, out _);
            _cacheLastUsed.TryRemove(key, out _);
        }
    }
}