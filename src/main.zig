const std = @import("std");
const Hive = @import("hive").Hive;

pub fn main() !void {
    var gpa_impl: std.heap.DebugAllocator(.{}) = .init;
    const gpa = gpa_impl.allocator();

    var hive: Hive(u32) = .empty;
    defer hive.deinit(gpa);

    var refs: std.ArrayList(Hive(u32).Reference) = .empty;
    defer refs.deinit(gpa);

    var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
    const rand = rng.random();

    for (0..100) |i| {
        const ref = try hive.insert(gpa, @intCast(i));
        std.debug.print("{}\n", .{ref});
        try refs.append(gpa, ref);
    }

    rand.shuffle(Hive(u32).Reference, refs.items);
    for (refs.items) |ref| {
        if (rand.boolean()) continue;
        std.debug.print("{}\n", .{ref});
        hive.erase(gpa, ref);
    }
}
