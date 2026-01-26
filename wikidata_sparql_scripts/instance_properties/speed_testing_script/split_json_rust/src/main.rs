use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};

const CHUNK_SIZE: usize = 500_000;
const BUFFER_SIZE: usize = 64 * 1024;

fn main() -> std::io::Result<()> {
    let input_file = "../extracted_data.json";
    let output_dir = "../extracted_batches_2";

    fs::create_dir_all(output_dir)?;

    let metadata = fs::metadata(input_file)?;
    let file_size = metadata.len();
    println!("File size: {} MB", file_size / 1024 / 1024);

    let file = File::open(input_file)?;
    let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);

    let mut file_num: usize = 1;
    let mut entry_count: usize = 0;
    let mut total_entries: usize = 0;
    let mut brace_depth: i32 = 0;
    let mut in_string = false;
    let mut escape_next = false;
    let mut bytes_read: u64 = 0;
    let mut last_percent: i32 = -1;

    let mut entry_buffer = String::with_capacity(50 * 1024);

    let filename = format!("{}/extracted_data_{:03}.json", output_dir, file_num);
    let mut out = BufWriter::new(File::create(&filename)?);
    writeln!(out, "{{")?;
    let mut first_in_file = true;

    let mut buf = [0u8; BUFFER_SIZE];
    let mut start_idx: usize = 0;
    let mut buf_len: usize = 0;
    let mut found_start = false;

    loop {
        // If we've processed the current buffer, read more
        if start_idx >= buf_len {
            buf_len = reader.read(&mut buf)?;
            if buf_len == 0 { break; }
            start_idx = 0;
        }

        for i in start_idx..buf_len {
            let c = buf[i];
            bytes_read += 1;
            start_idx = i + 1;

            // Still looking for initial {
            if !found_start {
                if c == b'{' {
                    brace_depth = 1;
                    found_start = true;
                }
                continue;
            }

            // Progress
            let percent = ((bytes_read * 100) / file_size) as i32;
            if percent != last_percent {
                print!("\rProgress: {}% | Entries: {} | File: {}   ", percent, total_entries, file_num);
                let _ = std::io::stdout().flush();
                last_percent = percent;
            }

            // Handle escape
            if escape_next {
                entry_buffer.push(c as char);
                escape_next = false;
                continue;
            }

            if c == b'\\' && in_string {
                entry_buffer.push(c as char);
                escape_next = true;
                continue;
            }

            // Handle strings
            if c == b'"' {
                in_string = !in_string;
                entry_buffer.push(c as char);
                continue;
            }

            if in_string {
                entry_buffer.push(c as char);
                continue;
            }

            // Track braces
            if c == b'{' {
                brace_depth += 1;
                entry_buffer.push(c as char);
            } else if c == b'}' {
                brace_depth -= 1;
                entry_buffer.push(c as char);

                if brace_depth == 1 {
                    if !first_in_file {
                        write!(out, ",\n")?;
                    }
                    first_in_file = false;
                    write!(out, "{}", entry_buffer)?;
                    entry_buffer.clear();

                    entry_count += 1;
                    total_entries += 1;

                    if entry_count >= CHUNK_SIZE {
                        writeln!(out, "\n}}")?;
                        out.flush()?;
                        println!("\nWrote extracted_data_{:03}.json ({} entries)", file_num, entry_count);

                        file_num += 1;
                        entry_count = 0;
                        let new_filename = format!("{}/extracted_data_{:03}.json", output_dir, file_num);
                        out = BufWriter::new(File::create(&new_filename)?);
                        writeln!(out, "{{")?;
                        first_in_file = true;
                    }
                } else if brace_depth == 0 {
                    break;
                }
            } else if brace_depth >= 1 {
                entry_buffer.push(c as char);
            }
        }
    }

    // Write remaining
    if entry_count > 0 {
        writeln!(out, "\n}}")?;
        out.flush()?;
        println!("\nWrote extracted_data_{:03}.json ({} entries)", file_num, entry_count);
    }

    println!("\n\nDone! Total: {} entries in {} files", total_entries, file_num);
    Ok(())
}
