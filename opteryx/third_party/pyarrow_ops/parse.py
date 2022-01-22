import numpy as np
import pyarrow as pa
import json, time

def json_to_struct(arr):
    return pa.array(np.vectorize(json.loads)(arr))

def text_extract(text, key):
    value = '"' + key + '":'
    start = text.find(value)
    if start > -1:
        end = text[start:].find((',' if ',' in text[start:] else '}'))
        return eval(text[start + len(value): start + end])
    else:
        return None

def json_extract(arr, key):
    f = np.vectorize(lambda t: text_extract(t, key))
    return pa.array(f(arr.to_numpy()))