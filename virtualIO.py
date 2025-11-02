import threading

class virtualIO:
    def __init__(self):
        self.lock = threading.RLock()
        self.io = bytearray()
        self.cursor = 0
    
    def read(self, n: int = -1):
        with self.lock:
            if n == -1:
                n = len(self.io)
            data = self.io[self.cursor:self.cursor + n]
            self.cursor += n
            return bytes(data)
    
    def write(self, data: bytes):
        with self.lock:
            if self.cursor > len(self.io):
                size = self.cursor - len(self.io)
                self.io.extend(b"\x00" * size)
            self.io[self.cursor:self.cursor + len(data)] = data
            self.cursor += len(data)
    
    def seek(self, n: int):
        with self.lock:
            self.cursor = n

if __name__ == "__main__":
    s = virtualIO()
    s.write(b"data")
    print(s.io)
    s.seek(2)
    print(s.read(2))

    s.seek(8)
    print(s.cursor)
    s.write(b"test")
    print(s.cursor)
    print(s.io)

    s.seek(0)
    print(s.read())

