__author__ = 'MBZ'

import pandas as pd
import sys
import string
from collections import deque

def remove_non_printable(str):
    return ''.join(s for s in str if s in string.printable)


k = 0
queue = deque()
with pd.HDFStore('store.h5') as store:
    with open("C:\\Users\\Mohammad\\Downloads\\2014_04_03_stream.txt", "rb") as f:
        byte = f.read(1)

        while byte != "":
            parts = []
            frame = bytearray()
            while byte != b'\n' and byte != b'':
                if byte == b'\x01':
                    parts.append(frame.copy())
                    frame.clear()
                else:
                    frame += byte
                byte = f.read(1)

            try:
                #[lat, long] = parts[2].decode('ascii').split(',')
                d = { 'timestamp': parts[6].decode('ascii'),
                      'id' : parts[0].decode('ascii'),
                      'user': parts[12].decode('ascii'),
                      #'lat': lat,
                      #'long': long,
                      'tweet': remove_non_printable(parts[1].decode('utf_8'))}

                queue.append(d)
                #df = pd.DataFrame(d, index=[k])
                #store.append('tweets', df, min_itemsize=150)
            except:
                print('exp')
                print(sys.exc_info())
                pass;
            finally:
                byte = f.read(1)

                k += 1
                if k%1000 == 0:
                    print(k)
                if k%100000 == 0:
                    data = pd.DataFrame.from_records(queue)
                    store.append('tweets', data, min_itemsize=150, index=False)
                    queue.clear()

