__author__ = 'MBZ'

import pandas as pd

with pd.HDFStore('store.h5') as input:
    df = input.select('tweets', chunksize=100000)
    print(df)

