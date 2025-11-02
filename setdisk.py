import os
import orjson
import struct
import pickle
import threading

from typing import Literal
from typing import BinaryIO


class SetDisk:
    def __init__(self, primary: BinaryIO, secondary: BinaryIO, primarySize: int, secondarySize: int, multiple: int, algo: Literal["json", "pickle"] = "json", log=lambda x: None): # 10 Mo 1024*1024 * 10
        self.primary = primary
        self.secondary = secondary
        self.cursorSecondary = 1
        self.primarySize = primarySize
        self.secondarySize = secondarySize
        self.lock = threading.RLock()
        self.algo = algo
        self.multiple = multiple
        self.log = log
        primary.seek(self.primarySize - 1)
        primary.write(b'\x00')
        secondary.seek(self.secondarySize - 1)
        secondary.write(b'\x00')
    
    def encode(self, x) -> bytes:
        if self.algo == "json":
            return orjson.dumps(x)
        
        elif self.algo == "pickle":
            return pickle.dumps(x)
        else:
            raise ""
    
    def decode(self, x):
        if self.algo == "json":
            return orjson.loads(x)
        
        elif self.algo == "pickle":
            return pickle.loads(x)
        else:
            raise ""

    
    def hash_address(self, value, max_size: int, multiple: int, seed: int=0) -> int:
        h = hash((value, seed)) & 0xFFFFFFFFFFFFFFFF  # 64 bits
        num_slots = max_size // multiple
        slot_index = h % num_slots
        return slot_index * multiple

    def _addPrimary(self, seed, adressSecondary: int):
        number_try = 0
        while number_try <= 1000:
            address = self.hash_address(seed, self.primarySize, self.multiple,  seed=number_try)
            self.log(f"\033[32m ADD Primary : {adressSecondary!r} at {address} \033[0m")
            with self.lock:
                self.primary.seek(address)
                header = self.primary.read(1)
                self.log(f"\033[32m ADD Primary : Header read at address {address}: {header} \033[0m")
                if header == b'\x00':
                    dataMETA = struct.pack("!I", adressSecondary)
                    if len(dataMETA)+1 <= self.multiple:
                        self.primary.seek(address)
                        self.primary.write(b'\x01')
                        self.primary.write(dataMETA)
                        #self.primary.write(str(adressSecondary).encode("utf-8"))
                    else:
                        raise RuntimeError("The block size (multiple) is too small")
                    return
                else:
                    self.log(f"\033[0;31m ADD Primary : Collision detected at address : {address} \033[0m")
            number_try += 1
        raise RuntimeError("ADD Primary : Too many collisions")
    
    def _addSecondary(self, value):
        data = struct.pack("!I", len(value)) + value
        data_size = len(data)
        with self.lock:
            if self.cursorSecondary + data_size <= self.secondarySize:
                self.log(f"\033[94m ADD Secondary : {value} at {self.cursorSecondary} \033[0m")
                self.secondary.seek(self.cursorSecondary)
                self.secondary.write(data)
                self.cursorSecondary += data_size
                return self.cursorSecondary - data_size
            else:
                raise RuntimeError("more space available in the secondary table")
    
    def _findPrimary(self, value):
        number_try = 0
        while number_try <= 1000:
            address = self.hash_address(value, self.primarySize, self.multiple, seed=number_try)
            self.log(f"\033[32m CHECK Primary :{value!r} at {address} \033[0m")
            self.primary.seek(address)
            header = self.primary.read(1)
            if header == b'\x01':
                data = self.primary.read(4)
                if len(data) >= 4:
                    addressTable2 = struct.unpack("!I", data)[0]
                    returne = self._findSecondary(addressTable2)
                    if returne:
                        return returne
            elif header == b"\x00":
                return False
            number_try += 1
        return False 
    
    def _findSecondary(self, adress):
        with self.lock:
            self.secondary.seek(adress)
            data = self.secondary.read(4)
            if len(data) >= 4:
                size = struct.unpack("!I", data)[0]
                return self.secondary.read(size)
    
    def __contains__(self, value) -> bool:
        value = self.encode(value)
        return self._findPrimary(value)

    def add(self, value):
        value = self.encode(value)
        with self.lock:
            #self.log(self._findPrimary(value))
            if self._findPrimary(value) == value:
                self.log("\033[32m ADD Primary : Duplicate detected")
            else:
                adress = self._addSecondary(value)
                self._addPrimary(value, adress)

    def remove(self, value):
        raise NotImplementedError("Remove operation is not yet available")


        

if __name__ == "__main__":
    import io
    import time
    if os.path.exists("setdisk.table.primary.db"):
        os.remove("setdisk.table.primary.db")

    if os.path.exists("setdisk.table.secondary.db"):
        os.remove("setdisk.table.secondary.db")
    
    with open("setdisk.table.primary.db", "wb") as f:
        pass
    with open("setdisk.table.secondary.db", "wb") as f:
        pass

    if True == False:
        log = lambda x: print(x)
    else:
        log = lambda x: None
    session = SetDisk(primary=io.BytesIO(),     # open("setdisk.table.primary.db", "r+b")
                      secondary=io.BytesIO(),   # open("setdisk.table.secondary.db", "r+b")
                      primarySize=1024 * 1024 * 1024, secondarySize=1024 * 1024 * 1024, multiple=6, log=log)  # 64 Ko
    
    print(session._findSecondary(1))

    #session._addPrimary(b"juan", 4294967295)
    
    session.add("juan")

    session.add("j" * 250)

    
    if "juan" in session:
        print("Exists")
    else:
        print("Not exists")
    
    start_time = time.monotonic()
    for i in range(5_000_000):
        session.add(f"value_{i}")
        if f"value_{i}" in session:
            pass
            #print("Exists")
        else:
            pass
            #print("Not exists")
    print(f"execution time : {time.monotonic() - start_time}")


    from virtualIO import virtualIO
    session = SetDisk(primary=virtualIO(),     # open("setdisk.table.primary.db", "r+b")
                      secondary=virtualIO(),   # open("setdisk.table.secondary.db", "r+b")
                      primarySize=1024 * 1024 * 1024, secondarySize=1024 * 1024 * 1024, multiple=6, log=log)
    start_time = time.monotonic()
    for i in range(5_000_000):
        session.add(f"value_{i}")
        if f"value_{i}" in session:
            pass
            #print("Exists")
        else:
            pass
            #print("Not exists")
    print(f"execution time with VirtualIO: {time.monotonic() - start_time}")


    sessionset = set()
    start_time = time.monotonic()
    for i in range(5_000_000):
        sessionset.add(f"value_{i}")
        if f"value_{i}" in sessionset:
            pass
            #print("Exists")
        else:
            pass
            #print("Not exists")
    print(f"execution time with Set: {time.monotonic() - start_time}")