const std = @import("std");

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
                next_free_block: usize,
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
                        .next_free_block = 0,
                        .next_segment = undefined,
                    },
                    .skip = skip,
                    .data = data,
                };
            }

            fn destroy(segment: *Segment, gpa: std.mem.Allocator) void {
                gpa.free(segment.skip[0..size(segment.head.capacity)]);
                segment.* = undefined;
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
        next_segment: usize,
        segments: std.MultiArrayList(Segment),
        reserve: ?*Segment,

        // NOTE on the construction of the free lists
        // there is a list of segments starting with next_segment in the hive structure
        // then in each segment there is a list of free blocks
        // the next_segment and next_free_block are indices into segments and data respectively
        // where std.math.maxInt(usize) (== nil) is used to indicate that there is no element
        // the free blocks in each segment form a doubly linked list
        // where loopback indices (e.g. a node at i pointing to i) is used to indicate no element

        pub const empty: Self = .{
            .total_capacity = 0,
            .len = 0,
            .next_segment = nil,
            .segments = .empty,
            .reserve = null,
        };

        pub fn deinit(hive: *Self, gpa: std.mem.Allocator) void {
            if (hive.reserve) |reserve| reserve.destroy(gpa);
            hive.* = undefined;
        }

        pub fn insert(hive: *Self, gpa: std.mem.Allocator, value: T) !Reference {
            if (hive.next_segment == nil) try hive.ensureUnusedCapacity(gpa, 1);
            // fetch segment data
            const ix_segment = hive.next_segment;
            const head = &hive.segments.items(.head)[ix_segment];
            const skip = hive.segments.items(.skip)[ix_segment];
            const data = hive.segments.items(.data)[ix_segment];
            const ix = head.next_free_block;
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
                head.next_free_block += 1;
            } else {
                // free block is exhausted
                std.debug.assert(data[ix].node.prev == ix);
                if (free_block.next != ix) {
                    data[free_block.next].node.prev = free_block.next;
                    head.next_free_block = data[ix].node.next;
                } else {
                    // segment is completely full
                    hive.next_segment = head.next_segment;
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
                segment.head.next_segment = hive.next_segment;
                hive.next_segment = @intCast(hive.segments.len);
                hive.segments.appendAssumeCapacity(segment);
                hive.total_capacity += capacity;
            }
        }

        pub fn erase(hive: *Self, gpa: std.mem.Allocator, ref: Reference) void {
            _ = gpa;
            const loc: Location = .fromReference(ref);
            const ix_segment: usize = @intCast(loc.segment);
            const ix: usize = loc.offset;

            const head = &hive.segments.items(.head)[ix_segment];
            const skip = hive.segments.items(.skip)[ix_segment];
            const data = hive.segments.items(.data)[ix_segment];

            const value = data[ix];
            hive.len -= 1;

            // there are four options for the free block
            // a) both neighbours occupied, form new free block
            // b/c) one neighbour occupied (left/right), extend free block
            // d) both neighbours free, merge into the free block on the left
            // and the way to determine the case is to look at the skipfields
            const skip_left = if (ix == 0) 0 else skip[ix - 1];
            const skip_right = skip[ix + 1]; // NOTE may index into the padding skipfield
            if (skip_left == 0 and skip_right == 0) {
                skip[ix] = 1;
                data[ix] = .{ .node = .{
                    .prev = @intCast(ix),
                    .next = if (head.next_free_block == nil)
                        @intCast(ix)
                    else
                        head.next_free_block,
                } };
                head.next_free_block = @intCast(ix);
            } else if (skip_left > 0 and skip_right == 0) {
                skip[ix - skip[ix - 1]] = skip_left + 1;
                skip[ix] = skip_left + 1;
            } else if (skip_left == 0 and skip_right < 0) {
                //
            } else if (skip_left > 0 and skip_right > 0) {
                //
            } else unreachable;

            // TODO test if the segment is empty

            return value;
        }
    };
}
