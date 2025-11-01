import cProfile
import pstats
import io
from setdisk import SetDisk
import time

session = SetDisk(
    primary=io.BytesIO(),
    secondary=io.BytesIO(),
    primarySize=1024*1024*10,
    secondarySize=1024*1024*10,
    multiple=6
)

def main():
    for i in range(100_000):
        session.add(f"value_{i}")
        if f"value_{i}" in session:
            pass

profiler = cProfile.Profile()
profiler.enable()

start_time = time.monotonic()
main()
print("Total time:", time.monotonic() - start_time)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats("cumtime")
stats.print_stats(40)
stats.print_stats("SetDisk")