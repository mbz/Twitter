__author__ = 'MBZ'

import pandas as pd
from collections import deque


def write_data(queue, store, label):
    data = pd.DataFrame.from_records(queue)
    store.append(label, data, min_itemsize=150, index=False)
    queue.clear()

k = 0
hash_count = 0
ment_count = 0
queue_hashtag = deque()
queue_mentions = deque()
with pd.HDFStore('_mentions.h5') as output_mentions:
    with pd.HDFStore('_hashtags.h5') as output_hashtag:
        with pd.HDFStore('store.h5') as input:
            for df in input.select('tweets', chunksize=10000):
                for row_index, row in df.iterrows():
                    d = { 'timestamp': row['timestamp'],
                          'id' : row['id'] }

                    tweet = row['tweet']
                    for word in tweet.split():
                        if len(word) > 1:
                            if word[0] == '#':
                                hash_count += 1
                                dd= d.copy()
                                dd['hashtag'] = word
                                queue_hashtag.append(dd)
                                #df = pd.DataFrame(dd, index=[hash_count])
                                #output_hashtag.append('hashtags', df, min_itemsize=150)
                                if hash_count%100000 == 0:
                                    write_data(queue_hashtag, output_hashtag, 'hashtags')
                            elif word[0] == '@':
                                ment_count += 1
                                dd = d.copy()
                                dd['mention'] = word
                                queue_mentions.append(dd)
                                #df = pd.DataFrame(dd, index=[ment_count])
                                #output_mentions.append('mention', df, min_itemsize=150)
                                if ment_count%100000 == 0:
                                    write_data(queue_mentions, output_mentions, 'mentions')

                    k += 1
                    if k%1000 == 0:
                        print(k)

            write_data(queue_hashtag, output_hashtag, 'hashtags')
            write_data(queue_mentions, output_mentions, 'mentions')
