#!/usr/bin/env python3
import shutil, os, sys

SRC = '/tmp/ship_client_patched.py'
DST = '/opt/zero2_device/ship_client.py'
BAK = '/opt/zero2_device/ship_client.py.bak.m16'

# Check download
if not os.path.exists(SRC):
    print('ERROR: patched file not found')
    sys.exit(1)

size = os.path.getsize(SRC)
if size < 10000:
    print(f'ERROR: file too small ({size} bytes)')
    sys.exit(1)

# Check it has M16 fixes
with open(SRC, 'r') as f:
    content = f.read()

checks = ['FIRST_COMPLETED', 'ensure_future', 'watchdog', 'max_reconnect_delay', 'os._exit']
missing = [c for c in checks if c not in content]
if missing:
    print(f'ERROR: missing markers: {missing}')
    sys.exit(1)

# Backup and replace
shutil.copy2(DST, BAK)
shutil.copy2(SRC, DST)

# Verify
with open(DST, 'r') as f:
    new_content = f.read()

print(f'PATCHED OK: {len(new_content)} chars, {new_content.count(chr(10))} lines')
print(f'FIRST_COMPLETED: {new_content.count("FIRST_COMPLETED")}')
print(f'watchdog: {new_content.count("watchdog")}')
print(f'max_reconnect_delay: {new_content.count("max_reconnect_delay")}')
