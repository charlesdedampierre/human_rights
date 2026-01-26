// JSON Splitter - splits extracted_data.json into batches of 500,000 entries
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BATCH_SIZE 500000
#define READ_BUF_SIZE 65536
#define ENTRY_BUF_SIZE 131072

int main() {
    system("mkdir -p ./extracted_batches_2");

    FILE *input = fopen("./extracted_data.json", "r");
    if (!input) {
        fprintf(stderr, "Error: Cannot open extracted_data.json\n");
        return 1;
    }

    // Get file size for progress
    fseek(input, 0, SEEK_END);
    long file_size = ftell(input);
    fseek(input, 0, SEEK_SET);

    char *read_buf = malloc(READ_BUF_SIZE);
    char *entry_buf = malloc(ENTRY_BUF_SIZE);
    char filename[256];

    int file_num = 1;
    long entry_count = 0;
    long total_entries = 0;
    int brace_depth = 0;
    int in_string = 0;
    int escape_next = 0;
    int first_in_file = 1;
    int found_start = 0;
    long entry_pos = 0;
    long bytes_read = 0;
    int last_percent = -1;

    // Create first output file
    sprintf(filename, "./extracted_batches_2/extracted_data_%03d.json", file_num);
    FILE *output = fopen(filename, "w");
    fprintf(output, "{\n");

    size_t n;
    while ((n = fread(read_buf, 1, READ_BUF_SIZE, input)) > 0) {
        for (size_t i = 0; i < n; i++) {
            char c = read_buf[i];
            bytes_read++;

            // Progress display
            int percent = (int)((bytes_read * 100) / file_size);
            if (percent != last_percent) {
                last_percent = percent;
                printf("\rProgress: %d%% | Entries: %ld | File: %d   ",
                       percent, total_entries, file_num);
                fflush(stdout);
            }

            if (!found_start) {
                if (c == '{') {
                    found_start = 1;
                    brace_depth = 1;
                }
                continue;
            }

            if (escape_next) {
                escape_next = 0;
                entry_buf[entry_pos++] = c;
                continue;
            }

            if (c == '\\' && in_string) {
                entry_buf[entry_pos++] = c;
                escape_next = 1;
                continue;
            }

            if (c == '"') {
                in_string = !in_string;
                entry_buf[entry_pos++] = c;
                continue;
            }

            if (in_string) {
                entry_buf[entry_pos++] = c;
                continue;
            }

            if (c == '{') {
                brace_depth++;
                entry_buf[entry_pos++] = c;
                continue;
            }

            if (c == '}') {
                brace_depth--;
                entry_buf[entry_pos++] = c;

                if (brace_depth == 1) {
                    // Complete entry - write it
                    entry_buf[entry_pos] = '\0';

                    if (!first_in_file) {
                        fprintf(output, ",\n");
                    }
                    first_in_file = 0;
                    fprintf(output, "%s", entry_buf);

                    entry_pos = 0;
                    entry_count++;
                    total_entries++;

                    // Check if we need to rotate files
                    if (entry_count >= BATCH_SIZE) {
                        fprintf(output, "\n}");
                        fclose(output);
                        printf("\nWrote file %d (%ld entries)\n", file_num, entry_count);

                        file_num++;
                        entry_count = 0;
                        first_in_file = 1;

                        sprintf(filename, "./extracted_batches_2/extracted_data_%03d.json", file_num);
                        output = fopen(filename, "w");
                        fprintf(output, "{\n");
                    }
                } else if (brace_depth == 0) {
                    // End of main JSON object
                    goto done;
                }
                continue;
            }

            if (brace_depth >= 1) {
                entry_buf[entry_pos++] = c;
            }
        }
    }

done:
    if (entry_count > 0) {
        fprintf(output, "\n}");
    }
    fclose(output);
    fclose(input);

    printf("\nDone! Total: %ld entries in %d files\n", total_entries, file_num);

    free(read_buf);
    free(entry_buf);
    return 0;
}
