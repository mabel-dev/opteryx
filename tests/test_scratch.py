def random_string(length: int = 32) -> str:
    import os
    import base64

    # we're creating a series of random bytes, 3/4 the length
    # of the string we want, base64 encoding it (which makes
    # it longer) and then returning the length of string
    # requested.
    b = os.urandom(-((length * -3) // 4))
    return base64.b64encode(b).decode("utf8")[:length]


for i in range(128):
    assert len(random_string(i)) == i, i
