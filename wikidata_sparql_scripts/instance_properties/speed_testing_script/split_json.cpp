#include <iostream>
#include <fstream>
#include <string>
#include <sys/stat.h>

const int CHUNK_SIZE = 500000;

int main() {
    std::string input_file = "extracted_data.json";
    std::string output_dir = "extracted_batches";

    mkdir(output_dir.c_str(), 0755);

    std::ifstream in(input_file, std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open " << input_file << std::endl;
        return 1;
    }

    in.seekg(0, std::ios::end);
    long long file_size = in.tellg();
    in.seekg(0, std::ios::beg);
    std::cout << "File size: " << (file_size / 1024 / 1024) << " MB" << std::endl;

    int file_num = 1;
    int entry_count = 0;
    int total_entries = 0;
    int brace_depth = 0;
    bool in_string = false;
    bool escape_next = false;
    long long bytes_read = 0;
    int last_percent = -1;

    std::ofstream out;
    char filename[256];
    snprintf(filename, sizeof(filename), "%s/extracted_data_%03d.json", output_dir.c_str(), file_num);
    out.open(filename);
    out << "{\n";
    bool first_in_file = true;

    std::string entry_buffer;
    entry_buffer.reserve(50 * 1024);

    char c;

    // Skip to first {
    while (in.get(c)) {
        bytes_read++;
        if (c == '{') break;
    }
    brace_depth = 1;

    while (in.get(c)) {
        bytes_read++;

        // Progress
        int percent = (bytes_read * 100) / file_size;
        if (percent != last_percent) {
            std::cout << "\rProgress: " << percent << "% | Entries: " << total_entries
                      << " | File: " << file_num << "   " << std::flush;
            last_percent = percent;
        }

        // Handle escape sequences
        if (escape_next) {
            entry_buffer += c;
            escape_next = false;
            continue;
        }

        if (c == '\\' && in_string) {
            entry_buffer += c;
            escape_next = true;
            continue;
        }

        // Handle strings
        if (c == '"') {
            in_string = !in_string;
            entry_buffer += c;
            continue;
        }

        if (in_string) {
            entry_buffer += c;
            continue;
        }

        // Track braces outside strings
        if (c == '{') {
            brace_depth++;
            entry_buffer += c;
        } else if (c == '}') {
            brace_depth--;
            entry_buffer += c;

            // Completed a top-level entry (depth back to 1)
            if (brace_depth == 1) {
                if (!first_in_file) {
                    out << ",\n";
                }
                first_in_file = false;
                out << entry_buffer;
                entry_buffer.clear();

                entry_count++;
                total_entries++;

                // Check if need new file
                if (entry_count >= CHUNK_SIZE) {
                    out << "\n}";
                    out.close();
                    std::cout << "\nWrote " << filename << " (" << entry_count << " entries)" << std::endl;

                    file_num++;
                    entry_count = 0;
                    snprintf(filename, sizeof(filename), "%s/extracted_data_%03d.json", output_dir.c_str(), file_num);
                    out.open(filename);
                    out << "{\n";
                    first_in_file = true;
                }
            } else if (brace_depth == 0) {
                // End of file
                break;
            }
        } else if (brace_depth == 1 && c == '"') {
            // Start of new key at depth 1
            entry_buffer += c;
        } else if (brace_depth >= 1) {
            entry_buffer += c;
        }
    }

    // Write remaining
    if (entry_count > 0) {
        out << "\n}";
        out.close();
        std::cout << "\nWrote " << filename << " (" << entry_count << " entries)" << std::endl;
    }

    std::cout << "\n\nDone! Total: " << total_entries << " entries in " << file_num << " files" << std::endl;
    return 0;
}
