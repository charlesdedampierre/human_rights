// ARM64 Assembly JSON Splitter for macOS
// Uses libc functions for portability

.global _main
.align 4

.data
input_file:     .asciz "./extracted_data.json"
output_dir:     .asciz "./extracted_batches_2"
mkdir_cmd:      .asciz "mkdir -p ./extracted_batches_2"
mode_r:         .asciz "r"
mode_w:         .asciz "w"
fmt_filename:   .asciz "./extracted_batches_2/extracted_data_%03d.json"
fmt_progress:   .asciz "\rProgress: %ld%% | Entries: %ld | File: %ld   "
fmt_wrote:      .asciz "\nWrote file %ld (%ld entries)\n"
fmt_done:       .asciz "\nDone! Total: %ld entries in %ld files\n"
json_open:      .asciz "{\n"
json_close:     .asciz "\n}"
comma_nl:       .asciz ",\n"

.data
.align 4
input_fp:       .quad 0
output_fp:      .quad 0
file_num:       .quad 0
entry_count:    .quad 0
total_entries:  .quad 0
brace_depth:    .quad 0
in_string:      .quad 0
escape_next:    .quad 0
first_in_file:  .quad 0
found_start:    .quad 0
entry_pos:      .quad 0
file_size:      .quad 0
bytes_read:     .quad 0
last_percent:   .quad 0

.bss
.align 4
read_buf:       .skip 65536
entry_buf:      .skip 131072
filename:       .skip 256

.text

_main:
    stp     x29, x30, [sp, #-16]!
    stp     x19, x20, [sp, #-16]!
    stp     x21, x22, [sp, #-16]!
    stp     x23, x24, [sp, #-16]!
    mov     x29, sp

    // Create output directory
    adrp    x0, mkdir_cmd@PAGE
    add     x0, x0, mkdir_cmd@PAGEOFF
    bl      _system

    // Initialize all state variables
    mov     x0, #1
    adrp    x1, file_num@PAGE
    add     x1, x1, file_num@PAGEOFF
    str     x0, [x1]

    mov     x0, #1
    adrp    x1, first_in_file@PAGE
    add     x1, x1, first_in_file@PAGEOFF
    str     x0, [x1]

    mov     x0, #-1
    adrp    x1, last_percent@PAGE
    add     x1, x1, last_percent@PAGEOFF
    str     x0, [x1]

    // Zero-initialize counters
    adrp    x1, bytes_read@PAGE
    add     x1, x1, bytes_read@PAGEOFF
    str     xzr, [x1]

    adrp    x1, total_entries@PAGE
    add     x1, x1, total_entries@PAGEOFF
    str     xzr, [x1]

    adrp    x1, entry_count@PAGE
    add     x1, x1, entry_count@PAGEOFF
    str     xzr, [x1]

    adrp    x1, entry_pos@PAGE
    add     x1, x1, entry_pos@PAGEOFF
    str     xzr, [x1]

    adrp    x1, brace_depth@PAGE
    add     x1, x1, brace_depth@PAGEOFF
    str     xzr, [x1]

    adrp    x1, in_string@PAGE
    add     x1, x1, in_string@PAGEOFF
    str     xzr, [x1]

    adrp    x1, escape_next@PAGE
    add     x1, x1, escape_next@PAGEOFF
    str     xzr, [x1]

    adrp    x1, found_start@PAGE
    add     x1, x1, found_start@PAGEOFF
    str     xzr, [x1]

    // Open input file
    adrp    x0, input_file@PAGE
    add     x0, x0, input_file@PAGEOFF
    adrp    x1, mode_r@PAGE
    add     x1, x1, mode_r@PAGEOFF
    bl      _fopen
    cbz     x0, exit_error
    adrp    x1, input_fp@PAGE
    add     x1, x1, input_fp@PAGEOFF
    str     x0, [x1]

    // Get file size
    adrp    x0, input_fp@PAGE
    add     x0, x0, input_fp@PAGEOFF
    ldr     x0, [x0]
    mov     x1, #0
    mov     x2, #2          // SEEK_END
    bl      _fseek

    adrp    x0, input_fp@PAGE
    add     x0, x0, input_fp@PAGEOFF
    ldr     x0, [x0]
    bl      _ftell
    adrp    x1, file_size@PAGE
    add     x1, x1, file_size@PAGEOFF
    str     x0, [x1]

    adrp    x0, input_fp@PAGE
    add     x0, x0, input_fp@PAGEOFF
    ldr     x0, [x0]
    mov     x1, #0
    mov     x2, #0          // SEEK_SET
    bl      _fseek

    // Create first output file
    bl      create_output

    // Write opening brace
    adrp    x0, json_open@PAGE
    add     x0, x0, json_open@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

main_loop:
    // Read chunk
    adrp    x0, read_buf@PAGE
    add     x0, x0, read_buf@PAGEOFF
    mov     x1, #1
    mov     x2, #65536
    adrp    x3, input_fp@PAGE
    add     x3, x3, input_fp@PAGEOFF
    ldr     x3, [x3]
    bl      _fread
    mov     x19, x0         // bytes read in x19
    cbz     x19, finish

    // Process buffer
    mov     x20, #0         // buffer position

process_loop:
    cmp     x20, x19
    b.ge    main_loop

    // Get byte
    adrp    x0, read_buf@PAGE
    add     x0, x0, read_buf@PAGEOFF
    ldrb    w21, [x0, x20]
    add     x20, x20, #1

    // Update bytes_read
    adrp    x0, bytes_read@PAGE
    add     x0, x0, bytes_read@PAGEOFF
    ldr     x1, [x0]
    add     x1, x1, #1
    str     x1, [x0]

    // Check found_start
    adrp    x0, found_start@PAGE
    add     x0, x0, found_start@PAGEOFF
    ldr     x1, [x0]
    cbnz    x1, process_char

    // Looking for {
    cmp     w21, #'{'
    b.ne    process_loop

    // Found it
    mov     x1, #1
    str     x1, [x0]
    adrp    x0, brace_depth@PAGE
    add     x0, x0, brace_depth@PAGEOFF
    str     x1, [x0]
    b       process_loop

process_char:
    // Print progress occasionally
    bl      print_progress

    // Check escape_next
    adrp    x0, escape_next@PAGE
    add     x0, x0, escape_next@PAGEOFF
    ldr     x1, [x0]
    cbz     x1, check_backslash
    str     xzr, [x0]
    bl      add_to_entry
    b       process_loop

check_backslash:
    cmp     w21, #'\\'
    b.ne    check_quote
    adrp    x0, in_string@PAGE
    add     x0, x0, in_string@PAGEOFF
    ldr     x1, [x0]
    cbz     x1, check_quote
    bl      add_to_entry
    adrp    x0, escape_next@PAGE
    add     x0, x0, escape_next@PAGEOFF
    mov     x1, #1
    str     x1, [x0]
    b       process_loop

check_quote:
    cmp     w21, #'"'
    b.ne    check_in_str
    adrp    x0, in_string@PAGE
    add     x0, x0, in_string@PAGEOFF
    ldr     x1, [x0]
    eor     x1, x1, #1
    str     x1, [x0]
    bl      add_to_entry
    b       process_loop

check_in_str:
    adrp    x0, in_string@PAGE
    add     x0, x0, in_string@PAGEOFF
    ldr     x1, [x0]
    cbz     x1, check_open_brace
    bl      add_to_entry
    b       process_loop

check_open_brace:
    cmp     w21, #'{'
    b.ne    check_close_brace
    adrp    x0, brace_depth@PAGE
    add     x0, x0, brace_depth@PAGEOFF
    ldr     x1, [x0]
    add     x1, x1, #1
    str     x1, [x0]
    bl      add_to_entry
    b       process_loop

check_close_brace:
    cmp     w21, #'}'
    b.ne    other_char
    adrp    x0, brace_depth@PAGE
    add     x0, x0, brace_depth@PAGEOFF
    ldr     x1, [x0]
    sub     x1, x1, #1
    str     x1, [x0]
    bl      add_to_entry

    cmp     x1, #1
    b.ne    check_zero
    bl      write_entry
    b       process_loop

check_zero:
    cmp     x1, #0
    b.eq    finish
    b       process_loop

other_char:
    adrp    x0, brace_depth@PAGE
    add     x0, x0, brace_depth@PAGEOFF
    ldr     x1, [x0]
    cmp     x1, #1
    b.lt    process_loop
    bl      add_to_entry
    b       process_loop

finish:
    // Write remaining
    adrp    x0, entry_count@PAGE
    add     x0, x0, entry_count@PAGEOFF
    ldr     x1, [x0]
    cbz     x1, close_all

    adrp    x0, json_close@PAGE
    add     x0, x0, json_close@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

close_all:
    // Close files
    adrp    x0, output_fp@PAGE
    add     x0, x0, output_fp@PAGEOFF
    ldr     x0, [x0]
    bl      _fclose

    adrp    x0, input_fp@PAGE
    add     x0, x0, input_fp@PAGEOFF
    ldr     x0, [x0]
    bl      _fclose

    // Print done
    adrp    x0, fmt_done@PAGE
    add     x0, x0, fmt_done@PAGEOFF
    adrp    x1, total_entries@PAGE
    add     x1, x1, total_entries@PAGEOFF
    ldr     x1, [x1]
    adrp    x2, file_num@PAGE
    add     x2, x2, file_num@PAGEOFF
    ldr     x2, [x2]
    bl      _printf

    mov     x0, #0
    b       exit_prog

exit_error:
    mov     x0, #1

exit_prog:
    ldp     x23, x24, [sp], #16
    ldp     x21, x22, [sp], #16
    ldp     x19, x20, [sp], #16
    ldp     x29, x30, [sp], #16
    ret

// Add byte w21 to entry buffer
add_to_entry:
    adrp    x0, entry_pos@PAGE
    add     x0, x0, entry_pos@PAGEOFF
    ldr     x1, [x0]
    adrp    x2, entry_buf@PAGE
    add     x2, x2, entry_buf@PAGEOFF
    strb    w21, [x2, x1]
    add     x1, x1, #1
    str     x1, [x0]
    ret

// Write entry to output
write_entry:
    stp     x29, x30, [sp, #-16]!
    stp     x19, x20, [sp, #-16]!

    // Check first
    adrp    x0, first_in_file@PAGE
    add     x0, x0, first_in_file@PAGEOFF
    ldr     x1, [x0]
    cbnz    x1, skip_comma

    adrp    x0, comma_nl@PAGE
    add     x0, x0, comma_nl@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

skip_comma:
    adrp    x0, first_in_file@PAGE
    add     x0, x0, first_in_file@PAGEOFF
    str     xzr, [x0]

    // Null terminate entry
    adrp    x0, entry_pos@PAGE
    add     x0, x0, entry_pos@PAGEOFF
    ldr     x1, [x0]
    adrp    x2, entry_buf@PAGE
    add     x2, x2, entry_buf@PAGEOFF
    strb    wzr, [x2, x1]

    // Write entry
    adrp    x0, entry_buf@PAGE
    add     x0, x0, entry_buf@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

    // Clear entry buffer
    adrp    x0, entry_pos@PAGE
    add     x0, x0, entry_pos@PAGEOFF
    str     xzr, [x0]

    // Increment counters
    adrp    x0, entry_count@PAGE
    add     x0, x0, entry_count@PAGEOFF
    ldr     x1, [x0]
    add     x1, x1, #1
    str     x1, [x0]

    adrp    x0, total_entries@PAGE
    add     x0, x0, total_entries@PAGEOFF
    ldr     x1, [x0]
    add     x1, x1, #1
    str     x1, [x0]

    // Check chunk limit
    adrp    x0, entry_count@PAGE
    add     x0, x0, entry_count@PAGEOFF
    ldr     x1, [x0]
    movz    x2, #0xA120
    movk    x2, #0x7, lsl #16
    cmp     x1, x2
    b.lt    write_done

    bl      rotate_file

write_done:
    ldp     x19, x20, [sp], #16
    ldp     x29, x30, [sp], #16
    ret

// Rotate to new file
rotate_file:
    stp     x29, x30, [sp, #-16]!

    // Close JSON
    adrp    x0, json_close@PAGE
    add     x0, x0, json_close@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

    // Close file
    adrp    x0, output_fp@PAGE
    add     x0, x0, output_fp@PAGEOFF
    ldr     x0, [x0]
    bl      _fclose

    // Print progress
    adrp    x0, fmt_wrote@PAGE
    add     x0, x0, fmt_wrote@PAGEOFF
    adrp    x1, file_num@PAGE
    add     x1, x1, file_num@PAGEOFF
    ldr     x1, [x1]
    adrp    x2, entry_count@PAGE
    add     x2, x2, entry_count@PAGEOFF
    ldr     x2, [x2]
    bl      _printf

    // Increment file num
    adrp    x0, file_num@PAGE
    add     x0, x0, file_num@PAGEOFF
    ldr     x1, [x0]
    add     x1, x1, #1
    str     x1, [x0]

    // Reset
    adrp    x0, entry_count@PAGE
    add     x0, x0, entry_count@PAGEOFF
    str     xzr, [x0]

    mov     x0, #1
    adrp    x1, first_in_file@PAGE
    add     x1, x1, first_in_file@PAGEOFF
    str     x0, [x1]

    // Create new file
    bl      create_output

    // Write opening
    adrp    x0, json_open@PAGE
    add     x0, x0, json_open@PAGEOFF
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    ldr     x1, [x1]
    bl      _fputs

    ldp     x29, x30, [sp], #16
    ret

// Create output file
create_output:
    stp     x29, x30, [sp, #-16]!

    adrp    x0, filename@PAGE
    add     x0, x0, filename@PAGEOFF
    adrp    x1, fmt_filename@PAGE
    add     x1, x1, fmt_filename@PAGEOFF
    adrp    x2, file_num@PAGE
    add     x2, x2, file_num@PAGEOFF
    ldr     w2, [x2]
    bl      _sprintf

    adrp    x0, filename@PAGE
    add     x0, x0, filename@PAGEOFF
    adrp    x1, mode_w@PAGE
    add     x1, x1, mode_w@PAGEOFF
    bl      _fopen
    adrp    x1, output_fp@PAGE
    add     x1, x1, output_fp@PAGEOFF
    str     x0, [x1]

    ldp     x29, x30, [sp], #16
    ret

// Print progress
print_progress:
    stp     x29, x30, [sp, #-16]!

    adrp    x0, bytes_read@PAGE
    add     x0, x0, bytes_read@PAGEOFF
    ldr     x1, [x0]
    mov     x2, #100
    mul     x1, x1, x2
    adrp    x0, file_size@PAGE
    add     x0, x0, file_size@PAGEOFF
    ldr     x2, [x0]
    udiv    x1, x1, x2      // percent

    adrp    x0, last_percent@PAGE
    add     x0, x0, last_percent@PAGEOFF
    ldr     x2, [x0]
    cmp     x1, x2
    b.eq    skip_print
    str     x1, [x0]

    adrp    x0, fmt_progress@PAGE
    add     x0, x0, fmt_progress@PAGEOFF
    // x1 already has percent
    adrp    x2, total_entries@PAGE
    add     x2, x2, total_entries@PAGEOFF
    ldr     x2, [x2]
    adrp    x3, file_num@PAGE
    add     x3, x3, file_num@PAGEOFF
    ldr     x3, [x3]
    bl      _printf

    // Flush stdout
    mov     x0, #0
    bl      _fflush

skip_print:
    ldp     x29, x30, [sp], #16
    ret
