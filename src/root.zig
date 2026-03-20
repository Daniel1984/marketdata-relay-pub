const std = @import("std");
const zimq = @import("zimq");

pub const Self = @This();

allocator: std.mem.Allocator,
stream_url: [:0]const u8,
mutex: std.Thread.Mutex,
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

    // set high water mark to limit memory usage when no consumers are connected
    try self.socket.?.set(.sndhwm, 50);

    // don't wait for unsent messages on close
    try self.socket.?.set(.linger, 0);

    // prevents queueing when no peer exists (With immediate = 1: send fails until a peer connects)
    try self.socket.?.set(.immediate, true);

    // prevents dead peers from keeping queues alive.
    try self.socket.?.set(.tcp_keepalive, 1);

    try self.socket.?.connect(self.stream_url);
    // try self.socket.?.bind(self.stream_url);
    std.debug.print("data stream connected!\n", .{});
}

fn reconnect(self: *Self) void {
    self.mutex.lock();
    defer self.mutex.unlock();

    // Always disconnect first to clear any broken socket state before reconnecting.
    // The previous guard (if socket != null return) was wrong: a failed send leaves
    // socket non-null but broken, so the guard turned reconnect into a no-op.
    self.disconnect();
    std.log.info("attempting to reconnect stream...", .{});

    // Try to reconnect with exponential backoff
    var attempts: u32 = 0;
    while (attempts < 10) {
        const backoff_ms = (@as(u64, attempts) + 1) * 2000; // 1s, 2s, 3s, 4s, 5s
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

pub fn publishMessage(self: *Self, pld: []u8) !void {
    if (self.socket) |socket| {
        socket.sendSlice(pld, .{}) catch |err| {
            std.log.err("write to stream err: {}", .{err});
            self.reconnect();
            if (self.socket) |retry_socket| {
                retry_socket.sendSlice(pld, .{}) catch |retry_err| {
                    std.log.err("reconnected failet to publish msg: {}", .{retry_err});
                    return retry_err;
                };
            } else {
                return error.ConnectionFailed;
            }
        };
    } else {
        self.reconnect();
        if (self.socket) |socket| {
            socket.sendSlice(pld, .{}) catch |err| {
                std.log.warn("failed to publish msg: {}", .{err});
                return err;
            };
        } else {
            return error.ConnectionFailed;
        }
    }
}

// fn sendWithTopic(self: *Self, socket: *zimq.Socket, topic: []const u8, pld: []const u8) !void {
//     _ = self;
//     // Send topic as first frame with SNDMORE flag
//     try socket.sendSlice(topic, .{ .sndmore = true });
//     // Send payload as second frame
//     try socket.sendSlice(pld, .{});
// }
