import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx
from hadrodb import HadroDB

tweet_data = opteryx.query(
    "SELECT * FROM 'testdata/flat/formats/parquet/tweets.parquet' LIMIT 1000"
)
hadro_tweets = HadroDB("testdata/hadro/tweets_short")

for tweet in tweet_data.fetchall(as_dicts=True):
    hadro_tweets.add(tweet)

print(len(hadro_tweets))

hadro_tweets.close()
