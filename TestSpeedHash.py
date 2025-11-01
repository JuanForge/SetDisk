import time
import xxhash

def fnv1a_64(s: str) -> int:
    h = 0xcbf29ce484222325  # FNV offset basis 64-bit
    for c in s:
        h ^= ord(c)
        h *= 0x100000001b3
        h &= 0xFFFFFFFFFFFFFFFF  # 64-bit overflow
    return h

def djb2(s: str) -> int:
    h = 5381
    for c in s:
        h = ((h << 5) + h) + ord(c)  # h*33 + c
        h &= 0xFFFFFFFFFFFFFFFF       # 64-bit
    return h



start_time = time.perf_counter_ns()
data = hash("data")
print(f"time for Hash : {time.perf_counter_ns() - start_time} ns, data : {data}")

start_time = time.perf_counter_ns()
data = xxhash.xxh64("data").intdigest()
print(f"time for xxhash : {time.perf_counter_ns() - start_time} ns, data : {data}")

start_time = time.perf_counter_ns()
data = fnv1a_64("data")
print(f"time for fnv1a_64 : {time.perf_counter_ns() - start_time} ns, data : {data}")

start_time = time.perf_counter_ns()
data = djb2("data")
print(f"time for djb2 : {time.perf_counter_ns() - start_time} ns, data : {data}")