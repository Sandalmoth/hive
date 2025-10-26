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
                const capacity: usize = std.math.clamp(
                    (hive.capacity * 13) >> 4,
                    segment_min_capacity,
                    std.math.maxInt(u16),
                );
                // TODO use reserve if available
                const ix = hive.segments.insertAssumeCapacity(.{
                    .array = try .init(gpa, capacity),
                    .next = undefined,
                    .prev = undefined,
                });
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

        const Iterator = struct {
            const Pair = struct {
                index: Index,
                value_ptr: *T,
            };

            segment: SkipArray(Segment, u32).Iterator,
            offset: SkipArray(T, u16).Iterator,

            pub fn next(it: *Iterator) ?Pair {
                const kv = it.offset.next() orelse {
                    it.offset = (it.segment.next() orelse return null).value_ptr.array.iterator();
                    return it.next();
                };
                return .{
                    .index = .fromLocation(.{
                        .segment = @intCast(it.segment.cursor),
                        .offset = kv.index,
                    }),
                    .value_ptr = kv.value_ptr,
                };
            }
        };

        pub fn iterator(hive: *Self) Iterator {
            return .{ .segment = hive.segments.iterator(), .offset = .dummy };
        }
    };
}

test "Hive" {
    var h: Hive(usize) = try .init(std.testing.allocator);
    defer h.deinit(std.testing.allocator);

    for (0..100) |i| {
        const ix = try h.insert(std.testing.allocator, i);
        std.debug.print("{}\n", .{ix.toLocation()});
    }

    var it = h.iterator();
    while (it.next()) |kv| {
        std.debug.print("{} {}\n", .{ kv.index.toLocation(), kv.value_ptr.* });
    }
}

pub fn OldHive(comptime T: type) type {
    const nil = std.math.maxInt(usize);
    const min_capacity = @max(1, 64 / @sizeOf(T));

    return struct {
        const Self = @This();

        pub const Reference = enum(u64) {
            _,

            fn fromLocation(it: Location) Reference {
                const bits: u64 = @bitCast(it);
                return @enumFromInt(bits);
            }
        };

        const Location = packed struct {
            offset: u16,
            segment: u48,

            fn fromReference(ix: Reference) Location {
                const bits: u64 = @intFromEnum(ix);
                return @bitCast(bits);
            }
        };

        comptime {
            std.debug.assert(@sizeOf(Reference) == @sizeOf(Location));
        }

        const Segment = struct {
            const Header = struct {
                capacity: usize,
                next_segment: usize,
                prev_segment: usize,
                first_free_block: usize,
            };

            const Node = struct {
                next: u16,
                prev: u16,
            };
            const Data = union {
                node: Node,
                value: T,
            };

            head: Header,
            skip: [*]u16, // capacity + 1
            data: [*]Data, // capacity

            fn create(gpa: std.mem.Allocator, capacity: u16) !Segment {
                const bytes = try gpa.alloc(u16, size(capacity));

                // (sub)allocate and setup skiplist
                const skip: [*]u16 = bytes.ptr;
                skip[0] = capacity;
                skip[capacity - 1] = capacity;
                skip[capacity] = 0;

                // (sub)allocate data segment and setup free-block
                const data: [*]Data = @ptrFromInt(std.mem.alignForward(
                    usize,
                    @intFromPtr(bytes.ptr) + @sizeOf(u16) * (capacity + 1),
                    @alignOf(T),
                ));
                std.debug.assert(@intFromPtr(data) + @sizeOf(Data) * capacity <=
                    @intFromPtr(bytes.ptr) + @sizeOf(u16) * bytes.len);
                data[0] = .{ .node = .{
                    .prev = 0,
                    .next = 0,
                } };

                return .{
                    .head = .{
                        .capacity = capacity,
                        .next_segment = undefined,
                        .prev_segment = nil,
                        .first_free_block = 0,
                    },
                    .skip = skip,
                    .data = data,
                };
            }

            fn destroy(segment: Segment, gpa: std.mem.Allocator) void {
                gpa.free(segment.skip[0..size(segment.head.capacity)]);
            }

            /// number of u16's required to store the skipfields and the values
            fn size(capacity: usize) usize {
                const n = @sizeOf(u16) * (capacity + 1) + @sizeOf(Data) * capacity + @alignOf(Data);
                return (n + @sizeOf(u16) - 1) / @sizeOf(u16);
            }
        };

        // i think we need total capacity to be able to ensure. if we only grow then we don't
        // kinda wish we didn't need both capacity and len but i don't see a way around it
        total_capacity: usize,
        len: usize,
        first_segment: usize,
        first_slot: usize,
        segments: std.MultiArrayList(Segment),
        reserve: ?Segment,

        // FIXME so there's yet anotehr issue here
        // lets say we want to iterate over all values
        // it's easy per segment since we have the skip list
        // but we need also a skip list for the segments array since it can have holes
        // might be rewrite time, clearly the atomic unit is a fixed size skippable array thing

        // though that's still not obviously possible
        // because the root one needs to be able to expand and might need a bigger skipfield

        // NOTE on the construction of the free lists
        // there is a list of segments starting with next_segment in the hive structure
        // then in each segment there is a list of free blocks
        // the next_segment and first_free_block are indices into segments and data respectively
        // where std.math.maxInt(usize) (== nil) is used to indicate that there is no element
        // the free blocks in each segment form a doubly linked list
        // where loopback indices (e.g. a node at i pointing to i) is used to indicate no element

        pub const empty: Self = .{
            .total_capacity = 0,
            .len = 0,
            .first_segment = nil,
            .first_slot = nil,
            .segments = .empty,
            .reserve = null,
        };

        pub fn deinit(hive: *Self, gpa: std.mem.Allocator) void {
            if (hive.reserve) |reserve| reserve.destroy(gpa);
            hive.* = undefined;
        }

        pub fn insert(hive: *Self, gpa: std.mem.Allocator, value: T) !Reference {
            if (hive.first_segment == nil) try hive.ensureUnusedCapacity(gpa, 1);
            // fetch segment data
            const ix_segment = hive.first_segment;
            const head = &hive.segments.items(.head)[ix_segment];
            const skip = hive.segments.items(.skip)[ix_segment];
            const data = hive.segments.items(.data)[ix_segment];
            const ix = head.first_free_block;
            std.debug.assert(skip[ix] > 0);
            std.debug.assert(skip[ix] == skip[ix + skip[ix] - 1]);
            const free_block = data[ix].node;
            const free_block_len = skip[ix];
            // update skip list
            skip[ix + 1] = skip[ix] - 1;
            if (skip[ix] > 2) skip[ix + skip[ix] - 1] -= 1;
            skip[ix] = 0;
            std.debug.assert(skip[ix + 1] < head.capacity - ix);
            // update erasure list
            if (free_block_len > 1) {
                data[ix + 1] = .{ .node = .{
                    .prev = @intCast(ix + 1),
                    .next = if (free_block.next != ix) free_block.next else @intCast(ix + 1),
                } };
                head.first_free_block += 1;
            } else {
                // free block is exhausted
                std.debug.assert(data[ix].node.prev == ix);
                if (free_block.next != ix) {
                    data[free_block.next].node.prev = free_block.next;
                    head.first_free_block = data[ix].node.next;
                } else {
                    // segment is completely full
                    hive.first_segment = head.next_segment;
                    head.next_segment = nil;
                }
            }
            data[ix] = .{ .value = value };
            hive.len += 1;

            return .fromLocation(.{
                .segment = @intCast(ix_segment),
                .offset = @intCast(ix),
            });
        }

        pub fn ensureUnusedCapacity(
            hive: *Self,
            gpa: std.mem.Allocator,
            additional_count: usize,
        ) !void {
            while (hive.len + additional_count > hive.total_capacity) {
                // allocate new segments until we have enough capacity
                const capacity: usize = @max(min_capacity, @min(
                    std.math.maxInt(u16),
                    (hive.total_capacity * 13) >> 3,
                ));
                try hive.segments.ensureUnusedCapacity(gpa, 1);
                var segment: Segment = try .create(gpa, @intCast(capacity));
                // prepend to list of segments with free slots
                segment.head.next_segment = hive.first_segment;
                if (hive.first_segment != nil) {
                    hive.segments.items(.head)[hive.first_segment].prev_segment =
                        @intCast(hive.segments.len);
                }
                hive.first_segment = @intCast(hive.segments.len);
                hive.segments.appendAssumeCapacity(segment);
                hive.total_capacity += capacity;
            }
        }

        pub fn erase(hive: *Self, gpa: std.mem.Allocator, ref: Reference) T {
            const loc: Location = .fromReference(ref);
            const ix_segment: usize = @intCast(loc.segment);
            const ix: usize = loc.offset;

            const head = &hive.segments.items(.head)[ix_segment];
            const skip = hive.segments.items(.skip)[ix_segment];
            const data = hive.segments.items(.data)[ix_segment];

            std.debug.print("skip before", .{});
            for (skip[0..head.capacity]) |s| std.debug.print(" {}", .{s});
            std.debug.print("\n", .{});

            const value = data[ix].value;
            hive.len -= 1;

            // there are four options for the free block
            // a) both neighbours occupied, form new free block
            // b/c) one neighbour occupied (left/right), extend free block
            // d) both neighbours free, merge into the free block on the left
            // and the way to determine the case is to look at the skipfields
            const skip_left = if (ix == 0) 0 else skip[ix - 1];
            const skip_right = skip[ix + 1]; // NOTE may index into the padding skipfield
            std.debug.print("{} {}\n", .{ skip_left, skip_right });
            if (skip_left == 0 and skip_right == 0) {
                skip[ix] = 1;
                data[ix] = .{ .node = .{
                    .prev = @intCast(ix),
                    .next = if (head.first_free_block == nil)
                        @intCast(ix)
                    else
                        @intCast(head.first_free_block),
                } };
                if (head.first_free_block == nil) {
                    if (hive.first_segment != nil) {
                        hive.segments.items(.head)[hive.first_segment].prev_segment = ix_segment;
                    }
                    head.next_segment = hive.first_segment;
                    hive.first_segment = ix_segment;
                }
                head.first_free_block = @intCast(ix);
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
                    .prev = if (old_block.prev != ix + 1) old_block.prev else @intCast(ix),
                    .next = if (old_block.next != ix + 1) old_block.next else @intCast(ix),
                } };
                // since the free block has moved one step over, update the linked list
                if (old_block.prev == ix + 1) {
                    head.first_free_block = @intCast(ix);
                } else {
                    data[old_block.prev].node.next = @intCast(ix);
                }
                if (old_block.next != ix + 1) {
                    data[old_block.next].node.prev = @intCast(ix);
                }
            } else if (skip_left > 0 and skip_right > 0) {
                const new_block_len = skip_left + skip_right + 1;
                skip[ix - skip[ix - 1]] = new_block_len;
                skip[ix + skip[ix + 1]] = new_block_len;
                const old_block = data[ix + 1].node;
                if (old_block.prev != ix + 1) {
                    data[old_block.prev].node.next = if (old_block.next != ix + 1)
                        old_block.next
                    else
                        @intCast(old_block.prev);
                }
                if (old_block.next != ix + 1) {
                    data[old_block.prev].node.prev = if (old_block.prev != ix + 1)
                        old_block.prev
                    else
                        @intCast(old_block.next);
                }
            } else unreachable;

            if (head.first_free_block == 0 and skip[ix] == head.capacity) {
                // unlink block from list of extant blocks
                if (head.prev_segment != nil) {
                    hive.segments.items(.head)[head.prev_segment].next_segment = head.next_segment;
                }
                if (head.next_segment != nil) {
                    hive.segments.items(.head)[head.next_segment].prev_segment = head.prev_segment;
                }
                // add the slot in the multiarray to the slot free list
                head.next_segment = hive.first_slot;
                hive.first_slot = ix_segment;
                // possibly keep memory block for future use
                if (hive.reserve == null) {
                    hive.reserve = .{
                        .head = head.*, // NOTE we only actualy care about capacity
                        .skip = skip,
                        .data = data,
                    };
                } else if (hive.reserve.?.head.capacity < head.capacity) {
                    hive.reserve.?.destroy(gpa);
                    hive.reserve = .{
                        .head = head.*, // NOTE we only actualy care about capacity
                        .skip = skip,
                        .data = data,
                    };
                } else (Segment{
                    .head = head.*,
                    .skip = skip,
                    .data = data,
                }).destroy(gpa);
            }

            std.debug.print("skip after", .{});
            for (skip[0..head.capacity]) |s| std.debug.print(" {}", .{s});
            std.debug.print("\n", .{});

            return value;
        }
    };
}
