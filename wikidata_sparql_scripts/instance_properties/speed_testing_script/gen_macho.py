#!/usr/bin/env python3
"""Generate a minimal Mach-O ARM64 executable that calls system()"""
import struct

def p64(x): return struct.pack('<Q', x)
def p32(x): return struct.pack('<I', x)
def pad16(s): return s.ljust(16, b'\x00')[:16]

# Actual ARM64 machine code instructions (hex)
# This code: loads address of string, calls _system, returns 0
code = bytes([
    # adrp x0, string@PAGE
    0x00, 0x00, 0x00, 0x10,
    # add x0, x0, string@PAGEOFF  
    0x00, 0xc0, 0x00, 0x91,
    # bl _system (will be fixed by dyld)
    0x00, 0x00, 0x00, 0x94,
    # mov x0, #0
    0x00, 0x00, 0x80, 0xd2,
    # ret
    0xc0, 0x03, 0x5f, 0xd6,
])

# Command string
cmdstr = b'echo "Hello from hand-written machine code!"\x00'

print("ARM64 machine code instructions:")
for i in range(0, len(code), 4):
    instr = code[i:i+4]
    print(f"  {instr.hex()} ", end="")
print()
print(f"\nString data: {cmdstr}")
print("\nA full working executable needs ~50KB of headers, symbols, and dyld info.")
print("The C compiler handles all this automatically.")
