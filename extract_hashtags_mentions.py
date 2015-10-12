__author__ = 'MBZ'

import pandas as pd

k = 0
hash_count = 0
ment_count = 0

with pd.HDFStore('_mentions.h5') as output_mentions:
    with pd.HDFStore('_hashtags.h5') as output_hashtag:
        with pd.HDFStore('store.h5') as input:
            for rec in input.select('tweets', chunksize=1):
                row = rec.iloc[0]
                d = { 'timestamp': row['timestamp'],
                      'id' : row['id'] }

                tweet = row['tweet']
                for word in tweet.split():
                    if len(word) > 1:
                        if word[0] == '#':
                            hash_count += 1
                            dd= d.copy()
                            dd['hashtag'] = word
                            df = pd.DataFrame(dd, index=[hash_count])
                            output_hashtag.append('hashtags', df, min_itemsize=150)
                        elif word[0] == '@':
                            ment_count += 1
                            dd = d.copy()
                            dd['mention'] = word
                            df = pd.DataFrame(dd, index=[ment_count])
                            output_mentions.append('mention', df, min_itemsize=150)

                k += 1
                if k%100 == 0:
                    print(k)

