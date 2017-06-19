# multiversion_map
A C++ multiversion map (ala `std::map`)

This is an implementation of a C++ container with a similar interface to
(and based on) `std::map` but with support for multiversion (timestamped)
reads.

In particular, all values in the map are tagged with a user-specified
timestamp (which defaults to a 64-bit integer), and reads can be performed
as-of a timestamp.

## Performance estimates

A `multiversion_map` adds a timestamp to every value in the map, and
additionally keeps alive a range of timestamps so that reads (typically via
an iterator) can see a consistent snapshot of data.  A `multiversion_map`
requires external read/write locking for concurrency control (just like
`std::map`).

The application is expected to supply a timestamp used for updates (the
"current timestamp") and specify a timestamp old enough that history before
that point can be discarded because there will be no reads before that
point (the "oldest timestamp")

When existing data is updated or erased, old versions remain visible to
readers with timestamps in the past.  These old versions are tracked and
incrementally cleaned up during update operations (and they can be cleaned
up manually, e.g., if updates quiesce).

In the notes below, we rely on a factor G, the "garbage ratio", which is
defined by:
```
G = count(active versions) / count(visible keys)
```

An estimate of G is:
```
G ~= 1 + (bytes per timestamp) * (current - oldest) / (bytes currently visible)
```

The memory size estimate is reasonably straightforward:
```
sizeof(mvmap) ~= G * sizeof(map) + (key bytes per timestamp) *  (current - oldest)
```

(Note that this estimate doesn't take into account the size of the timestamps themselves, or other overhead).

Here are some cost estimates for operations:

```
O(mvmap::insert) ~= 2 * O(map::insert) + 2 * O(map::erase)
O(mvmap::erase) ~= 3 * O(map::insert) + 2 * O(map::erase)
O(mvmap::at) ~= O(map::at)
O(mvmap::find) ~= O(map::find)
O(mvmap::iterator::++) ~= G * O(map::iterator::++)
O(mvmap::iterator::--) ~= G * O(map::iterator::--) + O(map::iterator::++)
```
