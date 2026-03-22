const std = @import("std");
const zimq = @import("zimq");

pub const Self = @This();

allocator: std.mem.Allocator,
stream_url: [:0]const u8,
mutex: std.Thread.Mutex,
reconnecting: std.atomic.Value(bool),
context: ?*zimq.Context,
socket: ?*zimq.Socket,

pub const Opts = struct {
    stream_url: []const u8 = "tcp://127.0.0.1:5555",
};

pub fn init(allocator: std.mem.Allocator, opts: Opts) !Self {
    return Self{
        .allocator = allocator,
        .stream_url = try allocator.dupeZ(u8, opts.stream_url),
        .mutex = std.Thread.Mutex{},
        .reconnecting = std.atomic.Value(bool).init(false),
        .context = null,
        .socket = null,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.stream_url);
    self.disconnect();
}

fn disconnect(self: *Self) void {
    if (self.socket) |sock| {
        sock.deinit();
        self.socket = null;
    }
    if (self.context) |ctx| {
        ctx.deinit();
        self.context = null;
    }
}

pub fn connect(self: *Self) !void {
    self.context = try zimq.Context.init();
    self.socket = try zimq.Socket.init(self.context.?, .@"pub");

    // Drop messages once the outbound queue exceeds this limit.
    // For market data, fresh data is more valuable than backlogged data.
    try self.socket.?.set(.sndhwm, 50);

    // Don't wait for unsent messages on close.
    try self.socket.?.set(.linger, 0);

    // Note: immediate=true was removed. With immediate=true, ZMQ drops
    // messages silently whenever the peer connection hasn't been fully
    // confirmed yet (e.g. during relay restart or brief network blip).
    // Those silent drops triggered false reconnect attempts which blocked
    // the WebSocket consume loop, causing the exchange to kill the
    // subscription due to missed pings.

    // Detect and clean up dead TCP connections.
    try self.socket.?.set(.tcp_keepalive, 1);

    try self.socket.?.connect(self.stream_url);
    std.debug.print("data stream connected!\n", .{});
}

fn doReconnect(self: *Self) void {
    defer self.reconnecting.store(false, .release);

    self.mutex.lock();
    defer self.mutex.unlock();

    self.disconnect();
    std.log.info("attempting to reconnect stream...", .{});

    var attempts: u32 = 0;
    while (attempts < 10) {
        const backoff_ms = (@as(u64, attempts) + 1) * 1000; // 1s, 2s, 3s, 4s, 5s
        std.Thread.sleep(backoff_ms * std.time.ns_per_ms);

        self.connect() catch |err| {
            attempts += 1;
            std.log.warn("stream reconnection attempt {} failed: {}", .{ attempts, err });
            continue;
        };

        std.log.info("stream reconnected!", .{});
        return;
    }

    std.log.err("failed to reconnect stream after {} attempts", .{attempts});
}

// Spawns reconnect in a background thread so the caller (the WebSocket
// consume loop) is never blocked. If a reconnect is already in progress,
// subsequent calls are ignored.
fn reconnect(self: *Self) void {
    if (self.reconnecting.swap(true, .acquire)) return;

    const thread = std.Thread.spawn(.{}, doReconnect, .{self}) catch |err| {
        std.log.err("failed to spawn reconnect thread: {}", .{err});
        self.reconnecting.store(false, .release);
        return;
    };
    thread.detach();
}

pub fn publishMessage(self: *Self, pld: []u8) void {
    if (self.socket) |socket| {
        // socket.sendSlice(pld, .{}) catch |err| {
        socket.sendSlice(pld, .{ .dont_wait = true }) catch |err| {
            std.log.err("write to stream err: {}", .{err});
            self.reconnect();
        };
    } else {
        self.reconnect();
    }
}
