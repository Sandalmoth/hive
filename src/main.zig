const std = @import("std");
const hive = @import("hive");

pub fn main() !void {
    var acc: usize = 0;
    std.debug.print(
        "N\th_fill\ta_fill\th_iter\ta_iter\th_erase\ta_erase\th_iter\ta_iter\th_fill\ta_fill\n",
        .{},
    );

    for ([_]usize{ 10, 100, 1000, 10_000, 100_000, 1000_000 }) |N| {
        var gpa_impl: std.heap.DebugAllocator(.{}) = .init;
        const gpa = gpa_impl.allocator();

        var h: hive.Hive(usize) = try .init(gpa);
        defer h.deinit(gpa);

        var a: std.ArrayList(usize) = .empty;
        defer a.deinit(gpa);

        var h_toerase: std.ArrayList(hive.Index) = .empty;
        defer h_toerase.deinit(gpa);

        var a_toerase: std.ArrayList(usize) = .empty;
        defer a_toerase.deinit(gpa);

        var rng = std.Random.DefaultPrng.init(@bitCast(std.time.microTimestamp()));
        const rand = rng.random();

        var timer = try std.time.Timer.start();

        for (0..N) |i| {
            const ix = try h.insert(gpa, @intCast(i));
            if (rand.boolean()) try h_toerase.append(gpa, ix);
        }
        const t_h_fill = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (0..N) |i| {
            const ix = a.items.len;
            try a.append(gpa, i);
            if (rand.boolean()) try a_toerase.append(gpa, ix);
        }
        const t_a_fill = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        var it = h.iterator();
        while (it.next()) |kv| acc +%= 3 * kv.value_ptr.*;
        const t_h_iterfull = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (a.items) |x| acc +%= 7 * x;
        const t_a_iterfull = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        // the array toerase fill style won't really work
        // was just identical for the sake of comparison
        // but we have to rewrite it to work properly
        a_toerase.clearRetainingCapacity();
        for (0..h_toerase.items.len) |i| {
            try a_toerase.append(gpa, rand.uintLessThan(usize, N - i));
        }

        // also shuffle the hive to make it similarly random
        rand.shuffle(hive.Index, h_toerase.items);

        timer.reset();

        for (h_toerase.items) |ix| _ = h.erase(gpa, ix);
        const t_h_erase = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (a_toerase.items) |ix| _ = a.swapRemove(ix);
        const t_a_erase = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        it = h.iterator();
        while (it.next()) |kv| acc +%= 3 * kv.value_ptr.*;
        const t_h_iterhalf = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (a.items) |x| acc +%= 7 * x;
        const t_a_iterhalf = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (0..h_toerase.items.len) |i| {
            _ = try h.insert(gpa, i);
        }
        const t_h_refill = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        for (0..h_toerase.items.len) |i| {
            try a.append(gpa, i);
        }
        const t_a_refill = @as(f64, @floatFromInt(timer.lap())) * 1e-6;

        std.debug.print(
            "{}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\t{d:.2}\n",
            .{
                N,
                t_h_fill,
                t_a_fill,
                t_h_iterfull,
                t_a_iterfull,
                t_h_erase,
                t_a_erase,
                t_h_iterhalf,
                t_a_iterhalf,
                t_h_refill,
                t_a_refill,
            },
        );
    }

    std.debug.print("{}\n", .{acc});
}
