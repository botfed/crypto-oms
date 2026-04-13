//! Latency profiler viewer for the MM engine.
//! Reads samples from the shared mmap file and displays percentile tables.
//!
//! Usage: cargo run --bin mm_profile

use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;

// Must match the layout in mm/mod.rs latency module
const GLOBAL_HEADER: usize = 8;
const METRIC_HEADER: usize = 16;
const SAMPLE_SIZE: usize = 8;
const SAMPLES_PER_METRIC: usize = 1_048_576;
const METRIC_SECTION: usize = METRIC_HEADER + SAMPLES_PER_METRIC * SAMPLE_SIZE;
const LATENCY_PATH: &str = "/tmp/mm_latency.bin";

const METRIC_NAMES: &[&str] = &["tick_fast", "t2d", "fair_calc", "vol_calc", "tick_slow", "t2t", "sign", "tick_end"];

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

fn metric_offset(metric: usize) -> usize {
    GLOBAL_HEADER + metric * METRIC_SECTION
}

fn render_frame(mmap: &memmap2::Mmap, start_time: Instant) -> String {
    let mut buf = String::with_capacity(2048);

    // Header
    let elapsed = start_time.elapsed().as_secs();
    let h = elapsed / 3600;
    let m = (elapsed % 3600) / 60;
    let s = elapsed % 60;
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
    let _ = writeln!(buf, "  MM Latency Profile   uptime: {:02}:{:02}:{:02}   {}", h, m, s, now);

    let num_metrics = u64::from_le_bytes(mmap[0..8].try_into().unwrap()) as usize;
    if num_metrics == 0 || num_metrics > 64 {
        let _ = writeln!(buf, "  Waiting for samples...");
        return buf;
    }

    // Compute total samples across all metrics
    let mut total_samples: u64 = 0;
    for mi in 0..num_metrics.min(METRIC_NAMES.len()) {
        let off = metric_offset(mi);
        let wp = u64::from_le_bytes(mmap[off..off + 8].try_into().unwrap());
        total_samples += wp;
    }
    let _ = writeln!(buf, "  total samples: {}   file: {}", total_samples, LATENCY_PATH);
    let _ = writeln!(buf);

    if total_samples == 0 {
        let _ = writeln!(buf, "  Waiting for samples...");
        return buf;
    }

    // Table header
    let _ = writeln!(buf,
        "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "metric", "min", "p1", "p5", "p25", "p50", "p75", "p95", "p99", "p99.9", "max", "n");
    let _ = writeln!(buf,
        "  {:-<9}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}",
        "", "", "", "", "", "", "", "", "", "", "", "");

    // Table rows — one per metric
    for (mi, name) in METRIC_NAMES.iter().enumerate() {
        if mi >= num_metrics { break; }
        let off = metric_offset(mi);
        let write_pos = u64::from_le_bytes(mmap[off..off + 8].try_into().unwrap());
        let capacity = u64::from_le_bytes(mmap[off + 8..off + 16].try_into().unwrap()) as usize;
        if write_pos == 0 || capacity == 0 { continue; }

        let n = write_pos.min(capacity as u64) as usize;
        let start = if write_pos > capacity as u64 { write_pos - capacity as u64 } else { 0 };

        let mut samples = Vec::with_capacity(n);
        for pos in start..write_pos {
            let idx = (pos % capacity as u64) as usize;
            let sample_off = off + METRIC_HEADER + idx * SAMPLE_SIZE;
            if sample_off + SAMPLE_SIZE > mmap.len() { break; }
            let value = u64::from_le_bytes(mmap[sample_off..sample_off + 8].try_into().unwrap());
            if value > 0 {
                samples.push(value);
            }
        }

        if samples.is_empty() { continue; }
        samples.sort_unstable();
        let sn = samples.len();

        let _ = writeln!(buf,
            "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
            name,
            fmt_ns(samples[0]),
            fmt_ns(percentile(&samples, 1.0)),
            fmt_ns(percentile(&samples, 5.0)),
            fmt_ns(percentile(&samples, 25.0)),
            fmt_ns(percentile(&samples, 50.0)),
            fmt_ns(percentile(&samples, 75.0)),
            fmt_ns(percentile(&samples, 95.0)),
            fmt_ns(percentile(&samples, 99.0)),
            fmt_ns(percentile(&samples, 99.9)),
            fmt_ns(samples[sn - 1]),
            sn,
        );
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

    // Wait for global header to be written
    loop {
        let num_metrics = u64::from_le_bytes(mmap[0..8].try_into().unwrap()) as usize;
        if num_metrics > 0 && num_metrics <= 64 { break; }
        eprintln!("Waiting for MM process to initialize profiling...");
        thread::sleep(Duration::from_secs(1));
    }

    let start_time = Instant::now();

    // Hide cursor
    {
        let mut out = stdout().lock();
        let _ = write!(out, "{}", CURSOR_HIDE);
        let _ = out.flush();
    }

    // Ctrl-C handler: restore cursor and exit immediately
    ctrlc::set_handler(move || {
        use std::os::unix::io::AsRawFd;
        let fd = std::io::stdout().as_raw_fd();
        unsafe { libc::write(fd, CURSOR_SHOW.as_ptr() as *const _, CURSOR_SHOW.len()); }
        std::process::exit(0);
    }).ok();

    loop {
        let raw = render_frame(&mmap, start_time);
        let frame = prepare_frame(&raw);

        {
            let mut out = stdout().lock();
            let _ = write!(out, "{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW);
            let _ = out.flush();
        }

        thread::sleep(Duration::from_secs(1));
    }
}
