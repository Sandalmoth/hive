const std = @import("std");

const min_capacity = 16;

pub fn Hive(comptime T: type) type {
    const nil = std.math.maxInt(u48);

    return struct {
        const Self = @This();

        pub const Reference = enum(u64) {
            _,

            fn toLocation(ix: Reference) Location {
                return @bitCast(ix);
            }
        };

        const Location = packed struct {
            segment: u32,
            offset: u16,
            _pad: u16,

            fn toReference(it: Location) Reference {
                return @bitCast(it);
            }
        };

        const Segment = struct {
            const Header = struct {
                capacity: usize,
                next: Location,
            };

            const Node = struct {
                next: u16,
                prev: u16,
            };

            head: Header,
            skip: [*]u16, // capacity + 1
            data: [*]union { node: Node, value: T }, // capacity

            fn create(gpa: std.mem.Allocator, capacity: usize) !*Segment {
                const bytes = try gpa.alloc(u16, size(capacity));
                const skip = @intFromPtr(bytes.ptr);
                const data = std.mem.alignForward(
                    usize,
                    skip + @sizeOf(u16) * (capacity + 1),
                    @alignOf(T),
                );
                std.debug.assert(
                    data + @sizeOf(T) * capacity <= @intFromPtr(bytes.ptr) + bytes.len,
                );
                return .{
                    .head = .{
                        .capacity = capacity,
                        .next = undefined, // FIXME TODO
                    },
                    .skip = @ptrFromInt(skip),
                    .data = @ptrFromInt(data),
                };
            }

            fn destroy(segment: *Segment, gpa: std.mem.Allocator) void {
                gpa.free(segment.skip[0..size(segment.head.capacity)]);
                segment.* = undefined;
            }

            /// number of u16's required to store the skipfields and the values
            fn size(capacity: usize) usize {
                const n = @sizeOf(u16) * (capacity + 1) + @sizeOf(T) * capacity + @alignOf(T);
                return (n + @sizeOf(u16) - 1) / @sizeOf(u16);
            }
        };

        // i think we need total capacity to be able to ensure. if we only grow then we don't
        // kinda wish we didn't need both capacity and len but i don't see a way around it
        total_capacity: usize,
        len: usize,
        next: u32,
        segments: std.MultiArrayList(Segment),
        reserve: ?*Segment,

        pub const empty: Self = .{
            .next = nil,
            .segments = .empty,
            .reserve = null,
        };

        pub fn insert(hive: *Self, gpa: std.mem.Allocator, value: T) !Reference {
            if (hive.next == nil) hive.ensureUnusedCapacity(gpa, 1);

            const head = hive.segments.items(.head)[hive.next];

            _ = value;
            return undefined;
        }

        pub fn ensureUnusedCapacity(
            hive: *Self,
            gpa: std.mem.Allocator,
            additional_count: usize,
        ) !u32 {
            while (hive.len + additional_count > hive.total_capacity) {
                // allocate new segments until we have enough capacity
                const capacity: usize = @max(min_capacity, @min(
                    std.math.maxInt(u16),
                    (hive.total_capacity * 13) >> 3,
                ));
                try hive.segments.ensureUnusedCapacity(gpa, 1);
                var segment: Segment = try .create(gpa, capacity);
                // prepend to list of segments with free slots
                segment.head.next.segment = hive.next;
                hive.next = hive.segments.len - 1;
                hive.segments.appendAssumeCapacity(segment);
                hive.total_capacity += capacity;
            }
        }
    };
}
