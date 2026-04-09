//! Latency profiler viewer for the MM engine.
//! Reads samples from the shared mmap file and displays percentile tables.
//!
//! Usage: cargo run --bin mm_profile

use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;

const HEADER_SIZE: usize = 64;
const SAMPLE_SIZE: usize = 16;
const LATENCY_PATH: &str = "/tmp/mm_latency.bin";

const METRIC_NAMES: &[&str] = &["tick_fast", "t2d", "fair_calc", "vol_calc", "tick_slow", "t2t"];

const CURSOR_HOME: &str = "\x1B[H";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const CLEAR_BELOW: &str = "\x1B[J";

fn fmt_ns(nanos: u64) -> String {
    if nanos < 1_000 {
        format!("{:>5} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:>5.1} us", nanos as f64 / 1e3)
    } else if nanos < 1_000_000_000 {
        format!("{:>5.1} ms", nanos as f64 / 1e6)
    } else {
        format!("{:>5.1}  s", nanos as f64 / 1e9)
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
    sorted[idx]
}

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 120 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    (120, 50)
}

/// Truncate lines to terminal width and append ERASE_EOL to prevent stale chars.
fn prepare_frame(raw: &str) -> String {
    let (cols, _) = term_size();
    let mut out = String::with_capacity(raw.len() + raw.lines().count() * 6);
    for line in raw.lines() {
        if line.len() > cols {
            out.push_str(&line[..cols]);
        } else {
            out.push_str(line);
        }
        out.push_str(ERASE_EOL);
        out.push('\n');
    }
    out
}

fn render_frame(
    write_pos: u64,
    capacity: usize,
    mmap: &memmap2::Mmap,
    start_time: Instant,
) -> String {
    let mut buf = String::with_capacity(2048);

    // Header
    let elapsed = start_time.elapsed().as_secs();
    let h = elapsed / 3600;
    let m = (elapsed % 3600) / 60;
    let s = elapsed % 60;
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
    let _ = writeln!(buf, "  MM Latency Profile   uptime: {:02}:{:02}:{:02}   {}", h, m, s, now);
    let _ = writeln!(buf, "  total samples: {}   file: {}", write_pos, LATENCY_PATH);
    let _ = writeln!(buf);

    if write_pos == 0 {
        let _ = writeln!(buf, "  Waiting for samples...");
        return buf;
    }

    // Read all available samples
    let mut by_metric: HashMap<u8, Vec<u64>> = HashMap::new();
    let start = if write_pos > capacity as u64 {
        write_pos - capacity as u64
    } else {
        0
    };

    for pos in start..write_pos {
        let idx = (pos % capacity as u64) as usize;
        let offset = HEADER_SIZE + idx * SAMPLE_SIZE;
        if offset + SAMPLE_SIZE > mmap.len() { break; }
        let metric = mmap[offset];
        let value = u64::from_le_bytes(mmap[offset + 8..offset + 16].try_into().unwrap());
        by_metric.entry(metric).or_default().push(value);
    }

    // Table header
    let _ = writeln!(buf,
        "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "metric", "min", "p1", "p5", "p25", "p50", "p75", "p95", "p99", "p99.9", "max", "n");
    let _ = writeln!(buf,
        "  {:-<9}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}",
        "", "", "", "", "", "", "", "", "", "", "", "");

    // Table rows
    for (id, name) in METRIC_NAMES.iter().enumerate() {
        if let Some(samples) = by_metric.get(&(id as u8)) {
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            let n = sorted.len();
            let _ = writeln!(buf,
                "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
                name,
                fmt_ns(sorted[0]),
                fmt_ns(percentile(&sorted, 1.0)),
                fmt_ns(percentile(&sorted, 5.0)),
                fmt_ns(percentile(&sorted, 25.0)),
                fmt_ns(percentile(&sorted, 50.0)),
                fmt_ns(percentile(&sorted, 75.0)),
                fmt_ns(percentile(&sorted, 95.0)),
                fmt_ns(percentile(&sorted, 99.0)),
                fmt_ns(percentile(&sorted, 99.9)),
                fmt_ns(sorted[n - 1]),
                n,
            );
        }
    }

    let _ = writeln!(buf);
    let _ = writeln!(buf, "  Ctrl-C to exit.");

    buf
}

fn main() {
    // Wait for the latency file to appear
    eprintln!("Waiting for {} ...", LATENCY_PATH);
    let file = loop {
        match std::fs::OpenOptions::new().read(true).open(LATENCY_PATH) {
            Ok(f) => break f,
            Err(_) => thread::sleep(Duration::from_secs(1)),
        }
    };

    let mmap = unsafe {
        memmap2::Mmap::map(&file).expect("failed to mmap")
    };

    // Wait for capacity to be written
    loop {
        let capacity = u64::from_le_bytes(mmap[8..16].try_into().unwrap()) as usize;
        if capacity > 0 { break; }
        eprintln!("Waiting for MM process to initialize profiling...");
        thread::sleep(Duration::from_secs(1));
    }
    let capacity = u64::from_le_bytes(mmap[8..16].try_into().unwrap()) as usize;

    let start_time = Instant::now();

    // Hide cursor
    {
        let mut out = stdout().lock();
        let _ = write!(out, "{}", CURSOR_HIDE);
        let _ = out.flush();
    }

    // Ctrl-C handler: restore cursor and exit immediately
    ctrlc::set_handler(move || {
        // Use raw write to avoid deadlock with stdout lock
        use std::os::unix::io::AsRawFd;
        let fd = std::io::stdout().as_raw_fd();
        unsafe { libc::write(fd, CURSOR_SHOW.as_ptr() as *const _, CURSOR_SHOW.len()); }
        std::process::exit(0);
    }).ok();

    loop {
        let write_pos = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        let raw = render_frame(write_pos, capacity, &mmap, start_time);
        let frame = prepare_frame(&raw);

        {
            let mut out = stdout().lock();
            let _ = write!(out, "{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW);
            let _ = out.flush();
        }

        thread::sleep(Duration::from_secs(1));
    }
}
