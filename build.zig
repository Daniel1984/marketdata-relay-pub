const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // 1. Create and expose our public module
    const mod = b.addModule("marketdata_relay_pub", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // 2. Get the zimq dependency (must be listed in build.zig.zon)
    const zimq_dep = b.dependency("zimq", .{
        .target = target,
        .optimize = optimize,
    });

    // 3. Make zimq available inside our module as @import("zimq")
    mod.addImport("zimq", zimq_dep.module("zimq"));
}
