const std = @import("std");
const zimq = @import("zimq");

pub const Self = @This();

allocator: std.mem.Allocator,
stream_url: [:0]const u8,
context: *zimq.Context,
socket: *zimq.Socket,

pub const Opts = struct {
    stream_url: []const u8 = "tcp://127.0.0.1:5555",
};

pub fn init(allocator: std.mem.Allocator, opts: Opts) !Self {
    const url = try allocator.dupeZ(u8, opts.stream_url);

    const ctx = try zimq.Context.init();
    errdefer ctx.deinit();

    const sock = try zimq.Socket.init(ctx, .@"pub");
    errdefer sock.deinit();

    return .{
        .allocator = allocator,
        .stream_url = url,
        .context = ctx,
        .socket = sock,
    };
}

pub fn connect(self: *Self) !void {
    // 🔥 REAL-TIME CONFIG
    try self.socket.set(.sndhwm, 1);
    try self.socket.set(.conflate, true);
    try self.socket.set(.linger, 0);
    try self.socket.set(.tcp_keepalive, 1);
    try self.socket.set(.sndtimeo, 0);

    try self.socket.connect(self.stream_url);

    std.log.info("Publisher connected to {s}", .{self.stream_url});
}

pub fn deinit(self: *Self) void {
    self.socket.deinit();
    self.context.deinit();
    self.allocator.free(self.stream_url);
}

pub fn publishMessage(self: *Self, msg: []const u8) void {
    self.socket.sendSlice(msg, .{ .dont_wait = true }) catch |err| {
        if (err == error.WouldBlock) return; // expected → drop
        std.log.err("publish error: {}", .{err});
    };
}
