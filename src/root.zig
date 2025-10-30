const std = @import("std");

fn SkipArray(comptime T: type, comptime Skip: type) type {
    return struct {
        const Self = @This();

        const Node = struct {
            next: Skip,
            prev: Skip,
        };
        const Data = union {
            node: Node,
            value: T,
        };

        capacity: usize,
        skip: [*]Skip, // capacity + 1
        data: [*]Data, // capacity
        first_free_block: ?Skip,

        fn init(gpa: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity <= std.math.maxInt(Skip));
            const backing_memory = try gpa.alloc(Skip, size(capacity));

            // (sub)allocate and setup skiplist
            const skip: [*]Skip = backing_memory.ptr;
            skip[0] = @intCast(capacity);
            skip[capacity - 1] = @intCast(capacity);
            skip[capacity] = 0;

            // (sub)allocate data segment and setup free-block
            const data: [*]Data = @ptrFromInt(std.mem.alignForward(
                usize,
                @intFromPtr(backing_memory.ptr) + @sizeOf(Skip) * (capacity + 1),
                @alignOf(T),
            ));
            data[0] = .{ .node = .{
                .prev = 0,
                .next = 0,
            } };

            return .{
                .capacity = capacity,
                .skip = skip,
                .data = data,
                .first_free_block = 0,
            };
        }

        fn deinit(array: *Self, gpa: std.mem.Allocator) void {
            const backing_memory = array.skip[0..size(array.capacity)];
            gpa.free(backing_memory);
            array.* = undefined;
        }

        fn expand(array: *Self, gpa: std.mem.Allocator, additional_capacity: usize) !void {
            const capacity = array.capacity + additional_capacity;
            std.debug.assert(capacity <= std.math.maxInt(Skip));
            const backing_memory = try gpa.alloc(Skip, size(capacity));

            const skip: [*]Skip = backing_memory.ptr;
            skip[capacity] = 0;
            const data: [*]Data = @ptrFromInt(std.mem.alignForward(
                usize,
                @intFromPtr(backing_memory.ptr) + @sizeOf(Skip) * (capacity + 1),
                @alignOf(T),
            ));

            @memcpy(skip, array.skip[0..array.capacity]);
            @memcpy(data, array.data[0..array.capacity]);

            // setup skip list and free block
            // a) the array does not end with a free block - init new and add to list
            // b) the array ends with a free block - extend it
            if (skip[array.capacity - 1] == 0) {
                skip[array.capacity] = @intCast(additional_capacity);
                skip[capacity - 1] = @intCast(additional_capacity);
                data[array.capacity] = .{ .node = .{
                    .prev = @intCast(array.capacity),
                    .next = array.first_free_block orelse @intCast(array.capacity),
                } };
                if (array.first_free_block) |first| {
                    data[first].node.prev = @intCast(array.capacity);
                }
                array.first_free_block = @intCast(array.capacity);
            } else {
                const new_block_len = skip[array.capacity - 1] + additional_capacity;
                skip[array.capacity - skip[array.capacity - 1]] = @intCast(new_block_len);
                skip[capacity - 1] = @intCast(new_block_len);
            }

            gpa.free(array.skip[0..size(array.capacity)]);

            array.capacity = capacity;
            array.skip = skip;
            array.data = data;
        }

        fn size(capacity: usize) usize {
            const sizeof_skip = @sizeOf(Skip) * (capacity + 1) + @alignOf(Skip);
            const sizeof_data = @sizeOf(Data) * (capacity + 1) + @alignOf(Data);
            return (sizeof_skip + sizeof_data + @sizeOf(Skip) - 1) / @sizeOf(Skip);
        }

        fn full(array: Self) bool {
            return array.first_free_block == null;
        }

        fn empty(array: Self) bool {
            return array.skip[0] == array.capacity;
        }

        fn insertAssumeCapacity(array: *Self, value: T) Skip {
            std.debug.assert(array.first_free_block != null);

            const ix = array.first_free_block.?;
            const skip = array.skip;
            const data = array.data;

            std.debug.assert(skip[ix] > 0);
            std.debug.assert(skip[ix] == skip[ix + skip[ix] - 1]);
            const free_block = data[ix].node;
            const free_block_len = skip[ix];

            skip[ix + 1] = skip[ix] - 1;
            if (skip[ix] > 2) skip[ix + skip[ix] - 1] -= 1;
            skip[ix] = 0;
            std.debug.assert(skip[ix + 1] < array.capacity - ix);

            data[ix] = .{ .value = value };

            if (free_block_len > 1) {
                data[ix + 1] = .{ .node = .{
                    .prev = ix + 1,
                    .next = if (free_block.next != ix) free_block.next else @intCast(ix + 1),
                } };
                if (free_block.next != ix) {
                    data[free_block.next].node.prev = ix + 1;
                }
                array.first_free_block.? += 1;
            } else {
                // free block is exhausted, remove from free list
                std.debug.assert(free_block.prev == ix);
                if (free_block.next != ix) {
                    data[free_block.next].node.prev = free_block.next;
                    array.first_free_block = free_block.next;
                } else {
                    // segment is completely full
                    array.first_free_block = null;
                }
            }

            return @intCast(ix);
        }

        fn erase(array: *Self, ix: Skip) T {
            const skip = array.skip;
            const data = array.data;

            std.debug.assert(skip[ix] == 0);
            const value = data[ix].value;

            const skip_left = if (ix == 0) 0 else skip[ix - 1];
            const skip_right = skip[ix + 1]; // NOTE may index into the padding skipfield
            // there are four options for the free block
            // a) both neighbours occupied, form new free block
            // b/c) one neighbour occupied (left/right), extend free block
            // d) both neighbours free, merge into the free block on the left
            // and the way to determine the case is to look at the skipfields
            if (skip_left == 0 and skip_right == 0) {
                skip[ix] = 1;
                data[ix] = .{ .node = .{
                    .prev = ix,
                    .next = array.first_free_block orelse ix,
                } };
                if (array.first_free_block) |first| array.data[first].node.prev = ix;
                array.first_free_block = ix;
            } else if (skip_left > 0 and skip_right == 0) {
                const new_block_len = skip_left + 1;
                skip[ix - skip[ix - 1]] = new_block_len;
                skip[ix] = new_block_len;
            } else if (skip_left == 0 and skip_right > 0) {
                const new_block_len = skip_right + 1;
                skip[ix + skip[ix + 1]] = new_block_len;
                skip[ix] = new_block_len;
                const old_block = data[ix + 1].node;
                data[ix] = .{ .node = .{
                    .prev = if (old_block.prev != ix + 1) old_block.prev else ix,
                    .next = if (old_block.next != ix + 1) old_block.next else ix,
                } };
                // since the free block has moved one step over, update the linked list
                if (old_block.prev == ix + 1) {
                    array.first_free_block = ix;
                } else {
                    data[old_block.prev].node.next = ix;
                }
                if (old_block.next != ix + 1) {
                    data[old_block.next].node.prev = ix;
                }
            } else if (skip_left > 0 and skip_right > 0) {
                const new_block_len = skip_left + skip_right + 1;
                skip[ix - skip[ix - 1]] = new_block_len;
                skip[ix + skip[ix + 1]] = new_block_len;
                // now remove the skip block on the right
                const old_block = data[ix + 1].node;
                if (old_block.prev != ix + 1) {
                    data[old_block.prev].node.next = if (old_block.next != ix + 1)
                        old_block.next
                    else
                        old_block.prev;
                } else {
                    array.first_free_block = if (old_block.next != ix + 1)
                        old_block.next
                    else
                        null;
                }
                if (old_block.next != ix + 1) {
                    data[old_block.next].node.prev = if (old_block.prev != ix + 1)
                        old_block.prev
                    else
                        old_block.next;
                }
            } else unreachable;

            return value;
        }

        fn getPtr(array: *Self, ix: Skip) *T {
            std.debug.assert(array.skip[ix] == 0);
            return &array.data[ix].value;
        }

        const Iterator = struct {
            const Pair = struct {
                index: Skip,
                value_ptr: *T,
            };

            const Cursor =
                switch (Skip) {
                    u16 => u32,
                    u32 => u64,
                    else => @compileError("unsupported skipfield size"),
                };

            array: *Self,
            cursor: Cursor,

            const dummy: Iterator = .{ .array = undefined, .cursor = std.math.maxInt(Cursor) };

            fn next(it: *Iterator) ?Pair {
                if (it.cursor == std.math.maxInt(Cursor) or it.cursor >= it.array.capacity) return null;
                const pair: Pair = .{
                    .index = @intCast(it.cursor),
                    .value_ptr = &it.array.data[it.cursor].value,
                };
                it.cursor += 1;
                it.cursor += it.array.skip[it.cursor];
                return pair;
            }
        };

        fn iterator(array: *Self) Iterator {
            return .{ .array = array, .cursor = array.skip[0] };
        }

        fn debugPrint(array: Self) void {
            std.debug.print("SkipArray <{s}>, ", .{@typeName(T)});
            std.debug.print("skipfield size: {s}, ", .{@typeName(Skip)});
            std.debug.print("capacity: {}\n", .{array.capacity});
            std.debug.print("  skip:", .{});
            for (array.skip[0 .. array.capacity + 1]) |skip| std.debug.print(" {}", .{skip});
            std.debug.print("\n", .{});
            std.debug.print("  freelist: ", .{});
            if (array.first_free_block) |first_free_block| {
                var i = first_free_block;
                while (true) {
                    const node = array.data[i].node;
                    std.debug.print("({} [{}] {})", .{ node.prev, i, node.next });
                    if (node.next == i) break;
                    std.debug.print("->", .{});
                    i = node.next;
                }
            } else {
                std.debug.print("[skiparray is full]", .{});
            }
            std.debug.print("\n", .{});
        }
    };
}

test "SkipArray" {
    const N = 10;
    const M = 10_000;
    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    // {
    //     var ixs: std.ArrayList(u16) = .empty;
    //     defer ixs.deinit(std.testing.allocator);

    //     var a: SkipArray(usize, u16) = try .init(std.testing.allocator, N);
    //     defer a.deinit(std.testing.allocator);
    //     a.debugPrint();
    //     try std.testing.expect(a.empty());

    //     std.debug.print("--- inserting ---\n", .{});
    //     for (0..N) |i| {
    //         const ix = a.insertAssumeCapacity(i);
    //         try ixs.append(std.testing.allocator, ix);
    //         a.debugPrint();
    //     }
    //     try std.testing.expect(a.full());

    //     std.debug.print("--- erasing ---\n", .{});
    //     rand.shuffle(u16, ixs.items);
    //     for (ixs.items) |ix| {
    //         _ = a.erase(ix);
    //         a.debugPrint();
    //     }
    //     try std.testing.expect(a.empty());
    // }

    {
        // repeatedly insert and erase, making sure contents are as expected
        const Pair = struct { index: u16, value: usize };
        var ixs: std.ArrayList(Pair) = .empty;
        defer ixs.deinit(std.testing.allocator);

        var a: SkipArray(usize, u16) = try .init(std.testing.allocator, N);
        defer a.deinit(std.testing.allocator);

        var n: usize = 0;
        var i: usize = 0;
        var it = a.iterator();

        for (0..M) |_| {
            const insert_to = rand.intRangeLessThan(usize, n + 1, N);
            const erase_to = rand.intRangeLessThan(usize, 0, insert_to);

            while (n < insert_to) : (n += 1) {
                // std.debug.print("inserting {}\n", .{i});
                const ix = a.insertAssumeCapacity(i);
                // a.debugPrint();
                try ixs.append(std.testing.allocator, .{ .index = ix, .value = i });
                i += 1;
            }

            it = a.iterator();
            while (it.next()) |kv| {
                var n_found: usize = 0;
                for (ixs.items) |kv2| {
                    if (kv.index != kv2.index) continue;
                    try std.testing.expect(kv.value_ptr.* == kv2.value);
                    n_found += 1;
                }
                try std.testing.expect(n_found == 1);
            }
            for (ixs.items) |kv| try std.testing.expect(a.getPtr(kv.index).* == kv.value);

            rand.shuffle(Pair, ixs.items);

            while (n > erase_to) : (n -= 1) {
                const kv = ixs.pop().?;
                // std.debug.print("erasing {}\n", .{kv.index});
                _ = a.erase(kv.index);
                // a.debugPrint();
            }

            it = a.iterator();
            while (it.next()) |kv| {
                var n_found: usize = 0;
                for (ixs.items) |kv2| {
                    if (kv.index != kv2.index) continue;
                    try std.testing.expect(kv.value_ptr.* == kv2.value);
                    n_found += 1;
                }
                try std.testing.expect(n_found == 1);
            }
            for (ixs.items) |kv| try std.testing.expect(a.getPtr(kv.index).* == kv.value);
        }
    }
}

test "SkipArray expand" {
    // TODO how should we test that this is expected behaviour? debugPrint looks fine though...
    {
        var a: SkipArray(usize, u16) = try .init(std.testing.allocator, 2);
        defer a.deinit(std.testing.allocator);
        // a.debugPrint();
        try a.expand(std.testing.allocator, 1);
        // a.debugPrint();
    }

    {
        var a: SkipArray(usize, u16) = try .init(std.testing.allocator, 2);
        defer a.deinit(std.testing.allocator);
        _ = a.insertAssumeCapacity(0);
        _ = a.insertAssumeCapacity(1);
        // a.debugPrint();
        try a.expand(std.testing.allocator, 1);
        // a.debugPrint();
    }

    {
        var a: SkipArray(usize, u16) = try .init(std.testing.allocator, 2);
        defer a.deinit(std.testing.allocator);
        _ = a.insertAssumeCapacity(0);
        // a.debugPrint();
        try a.expand(std.testing.allocator, 1);
        // a.debugPrint();
    }

    {
        var a: SkipArray(usize, u16) = try .init(std.testing.allocator, 2);
        defer a.deinit(std.testing.allocator);
        _ = a.insertAssumeCapacity(0);
        _ = a.insertAssumeCapacity(1);
        _ = a.erase(0);
        // a.debugPrint();
        try a.expand(std.testing.allocator, 1);
        // a.debugPrint();
    }
}

pub const Index = enum(u64) {
    _,

    fn toLocation(ix: Index) Location {
        const x: u64 = @intFromEnum(ix);
        return @bitCast(x);
    }

    fn fromLocation(loc: Location) Index {
        const x: u64 = @bitCast(loc);
        return @enumFromInt(x);
    }

    const Location = packed struct(u64) {
        offset: u16,
        _pad: u16 = 0xAAAA,
        segment: u32,
    };
};

pub fn Hive(comptime T: type) type {
    const segment_min_capacity = @max(1, 64 / @sizeOf(T));
    const segments_initial_capacity = 2;

    return struct {
        const Self = @This();

        const Segment = struct {
            array: SkipArray(T, u16),
            next: u32,
            prev: u32,
        };

        capacity: usize,
        len: usize,
        segments: SkipArray(Segment, u32),
        first_free_segment: ?u32,
        reserve: ?Segment,

        pub fn init(gpa: std.mem.Allocator) !Self {
            const segments = try SkipArray(Segment, u32).init(gpa, segments_initial_capacity);
            return .{
                .capacity = 0,
                .len = 0,
                .segments = segments,
                .first_free_segment = null,
                .reserve = null,
            };
        }

        pub fn deinit(hive: *Self, gpa: std.mem.Allocator) void {
            var it = hive.segments.iterator();
            while (it.next()) |kv| kv.value_ptr.array.deinit(gpa);
            hive.segments.deinit(gpa);
            if (hive.reserve) |*reserve| reserve.array.deinit(gpa);
            hive.* = undefined;
        }

        pub fn ensureUnusedCapacity(
            hive: *Self,
            gpa: std.mem.Allocator,
            additional_count: usize,
        ) !void {
            while (hive.capacity < hive.len + additional_count) {
                // allocate new segments until we have enough capacity
                if (hive.segments.full()) {
                    try hive.segments.expand(gpa, (hive.segments.capacity * 13) >> 4);
                }
                var capacity: usize = std.math.clamp(
                    (hive.capacity * 13) >> 4,
                    segment_min_capacity,
                    std.math.maxInt(u16),
                );
                // TODO use reserve if available
                var ix: u32 = undefined;
                if (hive.reserve) |reserve| {
                    ix = hive.segments.insertAssumeCapacity(reserve);
                    capacity = reserve.array.capacity;
                    hive.reserve = null;
                } else {
                    ix = hive.segments.insertAssumeCapacity(.{
                        .array = try .init(gpa, capacity),
                        .next = undefined,
                        .prev = undefined,
                    });
                }
                // stick the new segment on the list of segments with free slots
                const segment = hive.segments.getPtr(ix);
                segment.prev = ix;
                segment.next = hive.first_free_segment orelse ix;
                if (hive.first_free_segment) |free| hive.segments.getPtr(free).prev = ix;
                hive.first_free_segment = ix;

                hive.capacity += capacity;
            }
        }

        pub fn insert(hive: *Self, gpa: std.mem.Allocator, value: T) !Index {
            try hive.ensureUnusedCapacity(gpa, 1);
            return hive.insertAssumeCapacity(value);
        }

        pub fn insertAssumeCapacity(hive: *Self, value: T) Index {
            const first_free_segment = hive.first_free_segment.?;
            const segment = hive.segments.getPtr(first_free_segment);
            const offset = segment.array.insertAssumeCapacity(value);

            if (segment.array.full()) {
                if (segment.next == first_free_segment) {
                    std.debug.assert(segment.prev == first_free_segment);
                    hive.first_free_segment = null;
                } else {
                    hive.segments.getPtr(segment.next).prev = segment.next;
                    hive.first_free_segment = segment.next;
                }
            }

            hive.len += 1;

            return .fromLocation(.{
                .segment = first_free_segment,
                .offset = offset,
            });
        }

        pub fn erase(hive: *Self, gpa: std.mem.Allocator, ix: Index) T {
            const loc = ix.toLocation();
            const segment = hive.segments.getPtr(loc.segment);
            const was_full = segment.array.full();
            const value = segment.array.erase(loc.offset);

            if (was_full) {
                // add to segment free list
                if (hive.first_free_segment) |first| hive.segments.getPtr(first).prev = loc.segment;
                segment.next = hive.first_free_segment orelse loc.segment;
                segment.prev = loc.segment;
                hive.first_free_segment = loc.segment;
            }

            if (segment.array.empty()) {
                var s = hive.segments.erase(loc.segment);

                // remove from segment free list
                if (s.prev != loc.segment) {
                    hive.segments.getPtr(s.prev).next = if (s.next != loc.segment) s.next else s.prev;
                } else {
                    hive.first_free_segment = if (s.next != loc.segment) s.next else null;
                }
                if (s.next != loc.segment) {
                    hive.segments.getPtr(s.next).prev = if (s.prev != loc.segment) s.prev else s.next;
                }

                hive.capacity -= s.array.capacity;

                if (hive.reserve == null) {
                    hive.reserve = s;
                } else if (hive.reserve.?.array.capacity < s.array.capacity) {
                    hive.reserve.?.array.deinit(gpa);
                    hive.reserve = s;
                } else {
                    s.array.deinit(gpa);
                }
            }

            hive.len -= 1;

            return value;
        }

        pub fn getPtr(hive: *Self, ix: Index) *T {
            const loc = ix.toLocation();
            const segment = hive.segments.getPtr(loc.segment);
            return segment.array.getPtr(loc.offset);
        }

        const Iterator = struct {
            const Pair = struct {
                index: Index,
                value_ptr: *T,
            };

            ix_segment: u32, // need to store separately to construct index
            segment: SkipArray(Segment, u32).Iterator,
            offset: SkipArray(T, u16).Iterator,

            pub fn next(it: *Iterator) ?Pair {
                const kv = it.offset.next() orelse {
                    const segment = it.segment.next() orelse return null;
                    it.ix_segment = segment.index;
                    it.offset = segment.value_ptr.array.iterator();
                    return it.next();
                };
                return .{
                    .index = .fromLocation(.{
                        .segment = it.ix_segment,
                        .offset = kv.index,
                    }),
                    .value_ptr = kv.value_ptr,
                };
            }
        };

        pub fn iterator(hive: *Self) Iterator {
            return .{ .ix_segment = undefined, .segment = hive.segments.iterator(), .offset = .dummy };
        }

        fn debugPrint(hive: *Self) void {
            std.debug.print(
                "Hive <{s}>, len/capacity: {}/{}\n",
                .{ @typeName(T), hive.len, hive.capacity },
            );
            hive.segments.debugPrint();
            var it = hive.segments.iterator();
            while (it.next()) |segment| {
                std.debug.print(
                    "Segment, prev: {} next: {}, ",
                    .{ segment.value_ptr.prev, segment.value_ptr.next },
                );
                segment.value_ptr.array.debugPrint();
            }
            std.debug.print("segment freelist: ", .{});
            if (hive.first_free_segment) |first_free_block| {
                var i = first_free_block;
                while (true) {
                    const node = hive.segments.getPtr(i);
                    std.debug.print("({} [{}] {})", .{ node.prev, i, node.next });
                    if (node.next == i) break;
                    std.debug.print("->", .{});
                    i = node.next;
                }
            } else {
                std.debug.print("[hive is full]", .{});
            }
            std.debug.print("\n", .{});
        }
    };
}

test "Hive" {
    const N = 100;
    const M = 1_000;
    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    // var h2: Hive(usize) = try .init(std.testing.allocator);
    // defer h2.deinit(std.testing.allocator);

    // var ixs2: std.ArrayList(Index) = .empty;
    // defer ixs2.deinit(std.testing.allocator);

    // for (0..20) |i| {
    //     const ix = try h2.insert(std.testing.allocator, i);
    //     std.debug.print("{}\n", .{ix.toLocation()});
    //     try ixs2.append(std.testing.allocator, ix);
    // }

    // var it2 = h2.iterator();
    // while (it2.next()) |kv| {
    //     std.debug.print("{} {}\n", .{ kv.index.toLocation(), kv.value_ptr.* });
    // }

    // rand.shuffle(Index, ixs2.items);
    // h2.debugPrint();
    // for (ixs2.items) |ix| {
    //     std.debug.print("erasing {} {}\n", .{ ix.toLocation(), h2.erase(std.testing.allocator, ix) });
    //     h2.debugPrint();
    // }

    if (true) {
        // repeatedly insert and erase, making sure contents are as expected
        const Pair = struct { index: Index, value: usize };
        var ixs: std.ArrayList(Pair) = .empty;
        defer ixs.deinit(std.testing.allocator);

        var h: Hive(usize) = try .init(std.testing.allocator);
        defer h.deinit(std.testing.allocator);

        var n: usize = 0;
        var i: usize = 0;
        var it = h.iterator();

        for (0..M) |_| {
            const insert_to = rand.intRangeLessThan(usize, n + 1, N);
            const erase_to = rand.intRangeLessThan(usize, 0, insert_to);

            while (n < insert_to) : (n += 1) {
                const ix = try h.insert(std.testing.allocator, i);
                try ixs.append(std.testing.allocator, .{ .index = ix, .value = i });
                i += 1;
            }

            it = h.iterator();
            while (it.next()) |kv| {
                var n_found: usize = 0;
                for (ixs.items) |kv2| {
                    if (kv.index != kv2.index) continue;
                    try std.testing.expect(kv.value_ptr.* == kv2.value);
                    n_found += 1;
                }
                try std.testing.expectEqual(1, n_found);
            }
            for (ixs.items) |kv| try std.testing.expect(h.getPtr(kv.index).* == kv.value);

            rand.shuffle(Pair, ixs.items);

            while (n > erase_to) : (n -= 1) {
                const kv = ixs.pop().?;
                _ = h.erase(std.testing.allocator, kv.index);
            }

            it = h.iterator();
            while (it.next()) |kv| {
                var n_found: usize = 0;
                for (ixs.items) |kv2| {
                    if (kv.index != kv2.index) continue;
                    try std.testing.expect(kv.value_ptr.* == kv2.value);
                    n_found += 1;
                }
                try std.testing.expectEqual(1, n_found);
            }
            for (ixs.items) |kv| try std.testing.expect(h.getPtr(kv.index).* == kv.value);
        }
    }
}
