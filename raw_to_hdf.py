__author__ = 'MBZ'

import pandas as pd

k = 0
with pd.HDFStore('store.h5') as store:
    with open("C:\\Users\\Mohammad\\Downloads\\2014_04_03_stream.txt", "rb") as f:
        parts = []
        byte = f.read(1)
        frame = bytearray()
        while byte != "":
            while byte != b'\n' and byte != b'':
                if byte == b'\x01':
                    parts.append(frame.copy())
                    frame.clear()
                else:
                    frame += byte
                byte = f.read(1)

            try:
                [lat, long] = parts[2].decode('utf_8', 'ignore').split(',')
                d = { 'timestamp': parts[6].decode('utf_8', 'ignore'),
                      'id' : parts[0].decode('utf_8', 'ignore'),
                      'user': parts[12].decode('utf_8', 'ignore'),
                      'lat': lat,
                      'long': long,
                      'tweet': parts[1].decode('utf_8', 'ignore')}

                df = pd.DataFrame(d, index=[k])
                store.append('tweets', df)
            except:
                print('exp')
                pass;
            finally:
                k += 1
                if k%100 == 0:
                    print(k)
                byte = f.read(1)
