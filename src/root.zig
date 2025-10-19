const std = @import("std");

pub fn Hive(comptime T: type) type {
    const nil = std.math.maxInt(u48);
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
                next: Location,
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
                        .next = .{
                            .segment = undefined,
                            .offset = 0,
                        },
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
        next: u48,
        segments: std.MultiArrayList(Segment),
        reserve: ?*Segment,

        pub const empty: Self = .{
            .total_capacity = 0,
            .len = 0,
            .next = nil,
            .segments = .empty,
            .reserve = null,
        };

        pub fn deinit(hive: *Self, gpa: std.mem.Allocator) void {
            if (hive.reserve) |reserve| reserve.destroy(gpa);
            hive.* = undefined;
        }

        pub fn insert(hive: *Self, gpa: std.mem.Allocator, value: T) !Reference {
            if (hive.next == nil) try hive.ensureUnusedCapacity(gpa, 1);
            // fetch segment data
            const head = &hive.segments.items(.head)[hive.next];
            const skip = hive.segments.items(.skip)[hive.next];
            const data = hive.segments.items(.data)[hive.next];
            const ix = head.next.offset;
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
                    .prev = ix + 1,
                    .next = if (free_block.next != ix) free_block.next else ix + 1,
                } };
                head.next.offset += 1;
            } else {
                // free block is exhausted
                std.debug.assert(data[ix].node.prev == ix);
                if (free_block.next != ix) data[free_block.next].node.prev = free_block.next;
                head.next.offset = data[ix].node.next;
            }
            data[ix] = .{ .value = value };
            hive.len += 1;

            // TODO if the segment is full, remove it from the segment free list

            return .fromLocation(.{
                .segment = hive.next,
                .offset = ix,
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
                segment.head.next.segment = hive.next;
                hive.next = @intCast(hive.segments.len);
                hive.segments.appendAssumeCapacity(segment);
                hive.total_capacity += capacity;
            }
        }
    };
}
