const std = @import("std");
const Hive = @import("hive").Hive;

pub fn main() !void {
    var gpa_impl: std.heap.DebugAllocator(.{}) = .init;
    const gpa = gpa_impl.allocator();

    var hive: Hive(u32) = .empty;
    defer hive.deinit(gpa);
}
