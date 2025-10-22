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
        first_free_block: ?Skip,
        skip: [*]Skip, // capacity + 1
        data: [*]Data, // capacity

        fn create(gpa: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity <= std.math.maxInt(Skip));
            const backing_memory = try gpa.alloc(Skip, size(capacity));

            // (sub)allocate and setup skiplist
            const skip: [*]u16 = backing_memory.ptr;
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
                .first_free_block = 0,
                .skip = skip,
                .data = data,
            };
        }

        fn destroy(array: *Self, gpa: std.mem.Allocator) void {
            const backing_memory = array.skip[0..size(array.capacity)];
            gpa.free(backing_memory);
            array.* = undefined;
        }

        fn expand(array: *Self, gpa: std.mem.Allocator, additional_capacity: usize) void {
            _ = array;
            _ = gpa;
            _ = additional_capacity;
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
                    .prev = @intCast(ix + 1),
                    .next = if (free_block.next != ix) free_block.next else @intCast(ix + 1),
                } };
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

        const Iterator = struct {
            const Pair = struct {
                index: Skip,
                value_ptr: *T,
            };

            array: *Self,
            cursor: switch (Skip) {
                u16 => u32,
                u32 => u64,
                else => @compileError("unsupported skipfield size"),
            },

            fn next(it: *Iterator) ?Pair {
                if (it.cursor >= it.array.capacity) return null;
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
    var ixs: std.ArrayList(u16) = .empty;
    defer ixs.deinit(std.testing.allocator);
    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    var a: SkipArray(usize, u16) = try .create(std.testing.allocator, N);
    defer a.destroy(std.testing.allocator);
    a.debugPrint();
    try std.testing.expect(a.empty());

    std.debug.print("--- inserting ---\n", .{});
    for (0..N) |i| {
        const ix = a.insertAssumeCapacity(i);
        try ixs.append(std.testing.allocator, ix);
        a.debugPrint();
    }
    try std.testing.expect(a.full());

    var it = a.iterator();
    while (it.next()) |kv| {
        std.debug.print("{} {}, ", .{ kv.index, kv.value_ptr.* });
    }

    std.debug.print("--- erasing ---\n", .{});
    rand.shuffle(u16, ixs.items);
    for (ixs.items) |ix| {
        _ = a.erase(ix);
        a.debugPrint();
    }
    try std.testing.expect(a.empty());
}

pub fn Hive(comptime T: type) type {
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
