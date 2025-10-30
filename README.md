# Hive
An implementation of a datastructure along the lines of C++'s plf:colony or std::hive.
It has
- pointer stability, an inserted element is never moved
- _index_ stability, an index (obtained when inserting) can always be used to access the element
- efficient-ish iteration, data is stored in contiguous arrays using a skip-field for erased values

currently the API is pretty sparse but supports the most core and difficult to implement functions.

```zig
const h: Hive(usize) = .init(gpa);
defer h.deinit(gpa);

const ix = try h.insert(gpa, 123);

var it = h.iterator();
while (it.next()) |kv| std.debug.print("{} {}", .{kv.index, kv.value_ptr.*});

_ = h.erase(gpa, ix);
```

the performance currently seems to be a bit worse than plf/std.
