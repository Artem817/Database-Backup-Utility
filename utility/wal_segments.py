
SEGMENTS_PER_LOG = 0x100          
LOGS_PER_TIMELINE = 0x100000000

def get_next_wal_segment(current_wal: str) -> str:
    """Calculate the next WAL segment name given the current one."""
    timeline_hex = current_wal[0:8]
    log_hex = current_wal[8:16]
    seg_hex = current_wal[16:24]

    timeline = int(timeline_hex, 16)
    log = int(log_hex, 16)
    seg = int(seg_hex, 16)

    seg += 1
    if seg >= SEGMENTS_PER_LOG:
        seg = 0
        log += 1
        if log >= LOGS_PER_TIMELINE:
            log = 0
            timeline += 1

    return f"{timeline:08X}{log:08X}{seg:08X}"