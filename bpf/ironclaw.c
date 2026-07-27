//go:build ignore

#include <linux/types.h>
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

#define MAX_TRACKED_SYSCALLS 16384
#define RINGBUF_BYTES (256 * 1024)
#define MAX_ERRNO 4095
#define NANOS_PER_SECOND 1000000000ULL

char LICENSE[] SEC("license") = "GPL";

struct event {
    __u64 timestamp_ns;
    __u64 latency_ns;
    __s64 ret;
    __u32 pid;
    __u32 tgid;
    __s32 syscall_nr;
    __u8 failed;
    __u8 pad[3];
};

struct config {
    __u64 min_latency_ns;
    __u64 max_events_per_cpu_per_second;
    __u32 sample_modulus;
    __u32 target_tgid;
    __u8 enabled;
    __u8 pad[7];
};

struct rate_state {
    __u64 window_start_ns;
    __u64 emitted;
};

enum stat_index {
    STAT_OBSERVED = 0,
    STAT_EMITTED = 1,
    STAT_DROPPED_RINGBUF = 2,
    STAT_DROPPED_RATE = 3,
    STAT_DROPPED_FILTER = 4,
    STAT_DROPPED_UNPAIRED = 5,
    STAT_MAX = 6,
};

struct trace_event_sys_enter {
    __u64 unused;
    long id;
    unsigned long args[6];
};

struct trace_event_sys_exit {
    __u64 unused;
    long id;
    long ret;
};

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, RINGBUF_BYTES);
} events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, MAX_TRACKED_SYSCALLS);
    __type(key, __u64);
    __type(value, __u64);
} start_times SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct config);
} settings SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct rate_state);
} rate_limit SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, STAT_MAX);
    __type(key, __u32);
    __type(value, __u64);
} stats SEC(".maps");

static __always_inline void bump(__u32 index)
{
    __u64 *slot = bpf_map_lookup_elem(&stats, &index);
    if (slot)
        *slot += 1;
}

static __always_inline struct config *current_config(void)
{
    __u32 key = 0;
    return bpf_map_lookup_elem(&settings, &key);
}

static __always_inline int sampled_out(const struct config *cfg, __u32 tgid)
{
    if (cfg->sample_modulus <= 1)
        return 0;
    return (tgid % cfg->sample_modulus) != 0;
}

static __always_inline int rate_limited(const struct config *cfg, __u64 now)
{
    __u32 key = 0;
    struct rate_state *state = bpf_map_lookup_elem(&rate_limit, &key);
    if (!state)
        return 0;

    if (state->window_start_ns == 0 || now - state->window_start_ns >= NANOS_PER_SECOND) {
        state->window_start_ns = now;
        state->emitted = 0;
    }

    if (cfg->max_events_per_cpu_per_second && state->emitted >= cfg->max_events_per_cpu_per_second)
        return 1;

    state->emitted += 1;
    return 0;
}

SEC("tracepoint/raw_syscalls/sys_enter")
int ironclaw_sys_enter(struct trace_event_sys_enter *ctx)
{
    struct config *cfg = current_config();
    if (!cfg || !cfg->enabled)
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tgid = pid_tgid >> 32;

    if (cfg->target_tgid && tgid != cfg->target_tgid)
        return 0;
    if (sampled_out(cfg, tgid))
        return 0;

    __u64 now = bpf_ktime_get_ns();
    bpf_map_update_elem(&start_times, &pid_tgid, &now, BPF_ANY);

    return 0;
}

SEC("tracepoint/raw_syscalls/sys_exit")
int ironclaw_sys_exit(struct trace_event_sys_exit *ctx)
{
    struct config *cfg = current_config();
    if (!cfg || !cfg->enabled)
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();

    __u64 *started = bpf_map_lookup_elem(&start_times, &pid_tgid);
    if (!started) {
        bump(STAT_DROPPED_UNPAIRED);
        return 0;
    }

    __u64 now = bpf_ktime_get_ns();
    __u64 latency = now - *started;

    bpf_map_delete_elem(&start_times, &pid_tgid);
    bump(STAT_OBSERVED);

    if (latency < cfg->min_latency_ns) {
        bump(STAT_DROPPED_FILTER);
        return 0;
    }

    if (rate_limited(cfg, now)) {
        bump(STAT_DROPPED_RATE);
        return 0;
    }

    struct event *record = bpf_ringbuf_reserve(&events, sizeof(*record), 0);
    if (!record) {
        bump(STAT_DROPPED_RINGBUF);
        return 0;
    }

    record->timestamp_ns = now;
    record->latency_ns = latency;
    record->pid = (__u32)pid_tgid;
    record->tgid = pid_tgid >> 32;
    record->syscall_nr = (__s32)ctx->id;
    record->ret = (__s64)ctx->ret;
    record->failed = (ctx->ret < 0 && ctx->ret >= -MAX_ERRNO) ? 1 : 0;
    __builtin_memset(record->pad, 0, sizeof(record->pad));

    bpf_ringbuf_submit(record, 0);
    bump(STAT_EMITTED);

    return 0;
}
