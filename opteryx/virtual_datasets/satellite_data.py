# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
satellites
----------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

This data was obtained from:
https://github.com/devstronomy/nasa-data-scraper/blob/master/data/json/satellites.json

Licence @ 02-JAN-2022 when copied - MIT Licences attested, but data appears to be
from NASA, which is Public Domain.

To access this dataset you can either run a query against dataset :satellites: or you
can instantiate a SatelliteData() class and use it like a Relation.

This has a companion dataset, $planets, to help test joins.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.models import RelationStatistics

__all__ = ("read", "schema")


@single_item_cache
def read(*args):
    import io

    import pyarrow.parquet as pq
    import pybase64

    _decoded = pybase64.b64decode(
        b"UEFSMRUEFZAWFZ4ETBXiAhUEEgAAKLUv/WCIBC0IAIbWPh0QkK8dAAwADAAMAAwADEDJJZdccskll1xyySXTKTMANQA5ALty3ao1K9arVqtSnSo1KtSnTpsyXao0KdKjRosSHSo0KNCfPnvy3KkzJ86bNmvSnCkzJhVMEMEDDSyQwAEFDBDAv3778t2rNy/eu3br0p0rNy7ct27bsl2rNi3as2bLkh0rNizYr14Bb7SxRhpnlDFGGF90sUUWV1QxRRRPNLFEEkcUMUQQP/SwQw431DBDDC+0sEIKJ5QwQggfdLBBBhcE/zDB/NLLLrncUssssbzSyiqpnFLKKKF80skmmVxSySSRPNLIIokcUsgggfzRxx553FHHHHEEgLBUAgADARUAFYIDFZQDLBXiAhUEFQYVBhwYCLEAAAAAAAAAGAgBAAAAAAAAABYAKAixAAAAAAAAABgIAQAAAAAAAAAAAAAotS/9IMEJBgADAAAA4gIBCC8AAQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wMTIzNDU2Nzg5Ojs8PT4/QEFCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaW1xdXl9gYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4CBgoOEhYaHiImKi4yNjo+QkZKTlJWWl5iZmpucnZ6foKGio6SlpqeoqaqrrK2ur7AAAAAAAAAAJtwIHBUEGTUEAAYZGAJpZBUMFuICFrQaFtQIJsgEJggcGAixAAAAAAAAABgIAQAAAAAAAAAWACgIsQAAAAAAAAAYCAEAAAAAAAAAABksFQQVBBUCABUAFQQVAgAAABUEFXAVSEwVDhUEEgAAKLUv/SA43QAAoAMABAAFAAYABwAIAAkAAAAAAAAABlQCAAMBFQAVLBU+LBXiAhUEFQYVBhwYCAkAAAAAAAAAGAgDAAAAAAAAABYAKAgJAAAAAAAAABgIAwAAAAAAAAAAAAAotS/9IBaxAAADAAAA4gIBAwNIJEl8AnoDNgQcBQoGJrYMHBUEGTUEAAYZGAhwbGFuZXRJZBUMFuICFrQCFp4CJvwKJpgKHBgICQAAAAAAAAAYCAMAAAAAAAAAFgAoCAkAAAAAAAAAGAgDAAAAAAAAAAAZLBUEFQQVAgAVABUEFQIAAAAVBBW4HhWOEEwV4gIVBBIAACi1L/1gnAbtHwCKQUwLKvCKbHMDaUKpJuuh1FDKWqKlKfgehJ0gHsLt/1tAnj6JE5NNZOUnwBQhBaIArwCuAC+tCcXsamsSN/J/oVW1VCvl8dUw/DhakWW7yb7wY2iA3/YE5tSA0Fwuq06KPmUumyRq5krM/4bGdVNmHhzhUtow3LoR4Ea1RJjNHCppwzCN+K9QzcsXoTol2uLCK6Lit9F/hmarFual003+L3JdUkv530ht0SUV+e/gCNe5lPx3KFPqelngvyMrbmvyn8EG/gOATK06/GdAlrhd/jMes+W/IvtalQ46Wcoux3nhFhsH2cxeg/uSLkb+LxhWl0uOUJ1injqlaItKbTWxXlZUUm6XqFU+3tbqovx3vIyLSW0IeB3+L+DVomIslk6nN/n/gEktpOJAEPRAUMV5IAiBAQsCIA+EQECAQgKk4iQ8IKjiPM8DCRDBVkrzU+cuibJ0qrEmVadEs04pzW1ah6Bt6oXy243ThmHKEUqf5TGlGaVoiwvz4hKCPa6ihdxiH9jmqsWt0oH/jFyJSvC/4XQ7IXIYHkjkMDyRs/BEzsKDAYochQQochQiR+FBYIgchWdCJ6rVLrBJ+DorVmzLOdZEGd5OiNVouPvamJT/isgMXvNDP334z9hopdT58N+RLkJtWMppph/v8TZJG/47ejO6DTol/i+aurq9zo2q3O5Ji6nZbPNKLvLfsZ0L3LJnWf4jTFjbYDvzrBit6L9jInsZGGjQOZHrspdS8F/xWDOjkUibK0b/FZBbpVaIZtdmJf4fPG4GJOEoQJVYZUpxRmT0/0BbTCrhZV1SLI3cFwi3M0JjuizwtoLqAGG6bCyXrbZK/Gc8jttsK0vyfyGlJS5rveRb1hq1M9NpmeBOL/vBWdPbxrxslUbIkkaNWivI7dhk83RFKa1msi7/FcneugZ76xIygo3NUddZlceUMvhIthBqTy+3wWmrDvxv5DplzRL2stcSgJKoIRykpLRMiYhIo0pzYKIxRlUeEBjTNIUOWENclTSg6YMpA4Yk9sFdieA4RiUO3icitNZZmf5T6c8l0Fyfw4PLCJJBhLv2Z39AMlHrTz8Z/AgDTOMB5cmUn0XjmzP9NAGWSACKda0RDWRKbSQp5m1fV25focKu+Otk56OjdgTD6fqzqMh7D8NvElUXqrd4ljXGNtVQK5Ug8dceVLHR1DTxMhsoT2ww/nq0GhZ/nOmTRTQgnMQHu+vXC3bymjlE8ADJyxpzHYmzt7TCMJGlSfuUkjmtQBpvIPkyJQ0hR0QYuLVhwFl8+SaUUTb5mWmSH6eSwpcM9oYJhCxbZ102ZIyiyDIBerq7xzv/7DlXwo+8/3HKM/coWEznvDhe/DrzBaTjNqsKFQAVggMVlAMsFeICFQQVBhUGHDYAKARZbWlyGAhBZHJhc3RlYQAAACi1L/0gwQkGAAMAAADiAgEILwABAgMEBQYHCAkKCwwNDg8QERITFBUWFxgZGhscHR4fICEiIyQlJicoKSorLC0uLzAxMjM0NTY3ODk6Ozw9Pj9AQUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVpbXF1eX2BhYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ent8fX5/gIGCg4SFhoeIiYqLjI2Oj5CRkpOUlZaXmJmam5ydnp+goaKjpKWmp6ipqqusra6vsAAAAAAAAAAmlCIcFQwZNQQABhkYBG5hbWUVDBbiAhasIhaUFCawHiaADhw2ACgEWW1pchgIQWRyYXN0ZWEAGSwVBBUEFQIAFQAVBBUCAAAAFQQV4AwVqAtMFcwBFQQSAAAotS/9YDACVRYAdCcZBFYOzSazQGKjrN9MTEc/dmuZDMfzGT8j2/l+6ke3QH0/NV56BalAbxKDwOpPw0CLbOf7SQu8QESLbOf7qcE/zczMzMzM3D8ZBFYOLbKtP3sUrkfhepR0PxWMSuoENHE/L26jAbwFgj/8qfHSTWJgP7JjIxCv60c/mpmZmZmZuUCAP5C+SdOgaA4/e4UF9wMeCD+3Xz5ZMVztPqIm+nyUEec+8WjjiLX41D6N7bWg98bgPgWiJ2VSQ+s+VOQQcXMqyT7+2bA+AL7BFyZTBQRAyJi7lpDPHECmCkYldZpEQMgHPZtVR1JAtaZ5xyk+Y0AnoImwEYnBQF8HzhlR2tc/Y+5aQj4gXkDdtYR80LPhP+0NvjCZKsA/DeAtkKD4oT+eDI6SV+dIP4vgfyvZsTE/gJ9x4UBIJj9LsDic+dU8P5CDEmba/oU/sTOFzmvsgj84hCo1e6A1P8A+Qj8P1v85zJcnPxQsP8e6uI0G8FY/ycfuAiUF9j6Z8iGoGr0KP5T2Bl+YTGX4PlVAYFRAZmZmZmaGbEAMaEARQPp+arx0k2g/kst/SL99bT8TYcPTK2V5PxdIUPwYc5c/dEaU9gZfiD/OiNLe4AujP5p3nKIjubw/nMQgsHJokZg/1sVtNIC3yD8K16NwPQrHP0YldQKaCHs/4JwRpb3Bdz9bP6lNnNzvUDQ/0vvG155ZQj+8eapDboY7P2EyVTAqqVNDPy1DHOviNio/TpZAAEA5tMh2vp+KmT/sUbgehevBPwDQPx+F61G4HtU/4XoUrkfhCohrPzvfT42XbnI/3nGKjuTybz+6SQwCK4eGMz8zMzMzM5NZQFU/elI/HQDDJIs5Bp7agdojj4llYWwZ1qb1HcKdKYJjZxrKyj0NLfwByGbouNm9CbfWtha3zGwmQ2SxtoFWdmDIyAyJy7NUvpnMhAGPWMksO7O+lpUoFQAVsAIVwgIsFeICFQQVBhUGHBgIbxKDwOpPw0AYCAAAAAAAAACAFgAoCG8Sg8DqT8NAGAgAAAAAAAAAgAAAACi1L/0gmMEEAAMAAADiAgEHHYCAYEAoGA6IhGLBaDgekIhkQqlYLphMRnPZcDgcjUbT6XQ6HU2H09F0OBpOp9PpdDodjqfj6XQ6nU6n0+l8QCHRiFQynVAp1YrVcr1gnZjn4vHAZDNapVax3XC5is7j8Xg8Jh4NO17P9wMGhUNi0XhEJhXKJbPpfEKhUemUWrVesVntltv1fsFh8Zhc9gAAJrIyHBUKGTUEAAYZGAJnbRUMFuICFrIQFowPJvAuJqYjHBgIbxKDwOpPw0AYCAAAAAAAAACAFgAoCG8Sg8DqT8NAGAgAAAAAAAAAgAAZLBUEFQQVAgAVABUEFQIAAAAVBBXwCxX4BEwVvgEVBBIAACi1L/1g+AGVCQCkDAAAAAAAJptAMyZAzcwYQGZmZmZmdpxjmI6kQJqZmZmZ1KLcVEAAAAAAAEBVgEUAPjMyNywkpkhmIIA1MxEAEJkFBP4/AcwE+T8zC0AAAPg/8OA/xmiDb6iAjYHih0ApXI/CdR2k4GD8hqBaYFYMTZkxzChmJTMujEVZRCzM9D9mDuzTPyImQB5AGAwgMBQ0CBeCRYKniMuHeW0ZNDU7gERBSlEAQoBGQFTAUgA5OCooJZVAZYBAAFZAWGoAPzY13IJAAAAAAACAPkBZqBBYWzYOACOy2QEQEIM+S9P6m4tp9uF/Z37qXhpsl+K2zjYvpD35f76bdeGpNtxWQuy0wN46FjcceP3dmNmwkev8M3pZiKfHnNAu5EQu88S5fU7by47BNvR7O7+cVhya3SXYTsgVABXUAhXmAiwV4gIVBBUGFQYcGAhmZmZmZo6kQBgIMzMzMzMz0z8WACgIZmZmZmaOpEAYCDMzMzMzM9M/AAAAKLUv/SCqUQUAAwAAAOICAQcvgIBgQCgYDoiEYsFoOB6QiGRCqVgumExGc9lwOJxOp+PxeDyejofj6Xg4HY7H4/F4PB7Ox/PxeDwej8fj8YBCohGpZDqhUqoVq+V6wWIai2xGq9luuJxup+NRej6d7vf76Syc3+/3+/1+v9/vZ/lxfpYfBxgUDolF4xGZVC6ZTSfzCY3ybLfHQzq1UatW5POKzWp9W67C1m14HQ8AAAAAAAAm8DwcFQoZNQQABhkYBnJhZGl1cxUMFuICFuYPFoAJJoo5JvAzHBgIZmZmZmaOpEAYCDMzMzMzM9M/FgAoCGZmZmZmjqRAGAgzMzMzMzPTPwAZLBUEFQQVAgAVABUEFQIAAAAVBBWABRXEBEwVUBUEEgAAKLUv/WBAAMUIADQPjZduEoPACkDByqFFtvP9P/Cnxks3ifc/001iEFg5DEC0yHa+nxoIQN9PjZduEv8/8tJNYhBY/T9eukkMAivrP83MzMzMzARAAAhAZvI/7nw/NV66+T+JQWDl0CLvPwRWDi2ynffzP+kmMQisHP4/nMQgsHJo4T+HFtnO91PxP2iR7Xw/Nfo/KVyPwvUo5D97FK5H4XrkPwDgP3E9CtejcN0/uB6F61G43j9c3z/hehSuR+HaP8P1KFyPwtU/ZgJARrbz/dR4+fc/ZDvfT42X+j+LbOf7qfH4PwaBlUOLbPP0P/g/AEA5+gBAmpmZmZmZ6fY/DQBjyDc0L8FqYyuMYeCWlsfOpIRrt1KxIoa725kYNJcIIAoVABWmARW4ASwV4gIVBBUGFQYcGAjTTWIQWDkMQBgIw/UoXI/C1T8WAigI001iEFg5DEAYCMP1KFyPwtU/AAAAKLUv/SBTmQIABQAAAOACAQIABgNAIAxEYRwQCANJkiAIgiBcCAfKwjTOA0XSRFVVZV1YVlWVVlVKGwVc531gGIZhGIZhGIoQIgdhGI5iGIZhGIqiKIohWZonAAAm0kUcFQoZNQQABhkYB2RlbnNpdHkVDBbiAhbGBxacByaaQya2PhwYCNNNYhBYOQxAGAjD9Shcj8LVPxYCKAjTTWIQWDkMQBgIw/UoXI/C1T8AGSwVBBUEFQIAFQAVBBUCAAAAFQQVsAwV5gVMFcYBFQQSAAAotS/9YBgCTQsAog01NHBr2jC6vBTXCNqP4jfIINY9Rz546MQX9d2tnqc/0FWBXjd6Ze8kaRsQLSIxgyhYqo3DFWSf4ChxSyjURmAwvMyAPEppxx+y7U7a+0UFs/lEjzXcScs03ifveI930rLtvlTQXKiQxUsKLdvuwF8SCBwgC+dl3tqA1LkpnBZTN/OUKtyYlS7orkpQAB/6UQ+q6XasEmOZrZQyxwXnEUbr27M7TGInGUqTiPZ8xL1VLfgCGGOMJx4As/lEFYcPoEIVmkv17z216xtorbX2dCoIkIXzMgFXqLFoV8upU25QAlEqixsQEGPxA0PB29i420taI3dPy/OrZFO0qd1D87iIi7uUcsf670jbkCdYwLUmdN7gkzA1YqNn+MiYtEzQ44BxvgCvsDEcoh0kbYgtGtO2x3nIlWVbR1g0EbtnY+2ZtHnxnb/edwhLcN3SlK2Pj/PSx6atg+tndfqQkAA86PSlAL3SOjS4VBUAFdACFeICLBXiAhUEFQYVBhwYCAAAAAAAADtAGAh7FK5H4XopwBYMKAgAAAAAAAA7QBgIexSuR+F6KcAAAAAotS/9IKhBBQAIAAAAsAEBA8CiAQEHLYCAYEAoGA6IhGLBYDQciQckIplQKpbLBVPJaDacTgfj8Xw+IEyoIsKMSB8SqGQ6gUyf06iEEoVMoxIoNRKpVqyW6wWLyWAzWu1hu8lwlFyHhOLodiUTD6Xr+X46YFDYE6GEQyKwhxoSh0WhsGc8IpNH5ZLZdD6hUemUeoqAQr5qsGgtCa9YR1ar2XKpw263K/R+TeCwGAAAAAAmhFEcFQoZNQQABhkYCW1hZ25pdHVkZRUMFuICFqIQFuoJJqJNJppHHBgIAAAAAAAAO0AYCHsUrkfheinAFgwoCAAAAAAAADtAGAh7FK5H4XopwAAZLBUEFQQVAgAVABUEFQIAAAAVBBWgBhWqBUwVZBUEEgAAKLUv/WCQAF0KAGQRuB6F61G4vj+TGARWDi2yP5zEILByaLE/KVyPwvUo5D9xPQrXo3DlP4XrUbgehds/w/UoXI/CxT8K16NwPQq3P3sUrkfheqQ/qvHSTWIQqD+amZmZmZm5PwisHFpkO68/YhBYObTI7j8A9j9Ei2zn+6nzP1YOLbKd7+8/ke18PzVe7sk/MzMzMzMz0+M/I9v5fmq8tD/mP1zn+vD12eCuP/YoXI/C9dg/4XoUrkfhyj9I0c3UP+xRuB6F67E/pHA9CtejsLE/Gy/dJAaBtbM/O99PjZdusj/0/dR46SaxPzm0yHa+n7q6PzVeukkMAuc/18M/sp3vp8ZLtz/TTT/6fmq8dJO4PwIrhxbZztc/ZmZmZmZm1j8TABUrX2Uvamp5bCzLM8tgabJslTdLC0s52XG3oGOrd7IFd2cc1rzAKzPQSj5tlSNtQRQVABW4ARXKASwV4gIVBBUGFQYcGAi4HoXrUbj6PxgIexSuR+F6pD8WDCgIuB6F61G4+j8YCHsUrkfheqQ/AAAAKLUv/SBc4QIACAAAALABAQPAogEBBgNAIAxEYRwQCAOJsiAIgiBcCAlM4zxQJE1UZV1Ypk3bxnEcx3Ecx3EchyAuCAWd94GhKI5kaZInmiIQCAeiKq7s2R5u+yIIgiAKHMdxDAAm5FocFQoZNQQABhkYBmFsYmVkbxUMFuICFvgIFpQIJppYJtBSHBgIuB6F61G4+j8YCHsUrkfheqQ/FgwoCLgehetRuPo/GAh7FK5H4XqkPwAZLBUEFQQVAgAVABUEFQIAAAAVAhmcNQAYBnNjaGVtYRUQABUEJQIYAmlkABUEJQIYCHBsYW5ldElkABUMJQIYBG5hbWUlAEwcAAAAFQolAhgCZ20AFQolAhgGcmFkaXVzABUKJQIYB2RlbnNpdHkAFQolAhgJbWFnbml0dWRlABUKJQIYBmFsYmVkbwAW4gIZHBmMJtwIHBUEGTUEAAYZGAJpZBUMFuICFrQaFtQIJsgEJggcGAixAAAAAAAAABgIAQAAAAAAAAAWACgIsQAAAAAAAAAYCAEAAAAAAAAAABksFQQVBBUCABUAFQQVAgAAACa2DBwVBBk1BAAGGRgIcGxhbmV0SWQVDBbiAha0AhaeAib8CiaYChwYCAkAAAAAAAAAGAgDAAAAAAAAABYAKAgJAAAAAAAAABgIAwAAAAAAAAAAGSwVBBUEFQIAFQAVBBUCAAAAJpQiHBUMGTUEAAYZGARuYW1lFQwW4gIWrCIWlBQmsB4mgA4cNgAoBFltaXIYCEFkcmFzdGVhABksFQQVBBUCABUAFQQVAgAAACayMhwVChk1BAAGGRgCZ20VDBbiAhayEBaMDybwLiamIxwYCG8Sg8DqT8NAGAgAAAAAAAAAgBYAKAhvEoPA6k/DQBgIAAAAAAAAAIAAGSwVBBUEFQIAFQAVBBUCAAAAJvA8HBUKGTUEAAYZGAZyYWRpdXMVDBbiAhbmDxaACSaKOSbwMxwYCGZmZmZmjqRAGAgzMzMzMzPTPxYAKAhmZmZmZo6kQBgIMzMzMzMz0z8AGSwVBBUEFQIAFQAVBBUCAAAAJtJFHBUKGTUEAAYZGAdkZW5zaXR5FQwW4gIWxgcWnAcmmkMmtj4cGAjTTWIQWDkMQBgIw/UoXI/C1T8WAigI001iEFg5DEAYCMP1KFyPwtU/ABksFQQVBBUCABUAFQQVAgAAACaEURwVChk1BAAGGRgJbWFnbml0dWRlFQwW4gIWohAW6gkmok0mmkccGAgAAAAAAAA7QBgIexSuR+F6KcAWDCgIAAAAAAAAO0AYCHsUrkfheinAABksFQQVBBUCABUAFQQVAgAAACbkWhwVChk1BAAGGRgGYWxiZWRvFQwW4gIW+AgWlAgmmlgm0FIcGAi4HoXrUbj6PxgIexSuR+F6pD8WDCgIuB6F61G4+j8YCHsUrkfheqQ/ABksFQQVBBUCABUAFQQVAgAAABaMgAEW4gIm3AgWrFAUAAAZHBgMQVJST1c6c2NoZW1hGIAFLy8vLy85Z0JBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQWdBQUFCMEFRQUFMQUVBQVB3QUFBRElBQUFBbUFBQUFHZ0FBQUEwQUFBQUJBQUFBTHorLy84QUFBRURFQUFBQUJnQUFBQUVBQUFBQUFBQUFBWUFBQUJoYkdKbFpHOEFBRTcvLy84QUFBSUE2UDcvL3dBQUFRTVFBQUFBSEFBQUFBUUFBQUFBQUFBQUNRQUFBRzFoWjI1cGRIVmtaUUFBQUg3Ly8vOEFBQUlBR1AvLy93QUFBUU1RQUFBQUdBQUFBQVFBQUFBQUFBQUFCd0FBQUdSbGJuTnBkSGtBcXYvLy93QUFBZ0JFLy8vL0FBQUJBeEFBQUFBWUFBQUFCQUFBQUFBQUFBQUdBQUFBY21Ga2FYVnpBQURXLy8vL0FBQUNBSEQvLy84QUFBRURFQUFBQUJ3QUFBQUVBQUFBQUFBQUFBSUFBQUJuYlFBQUFBQUdBQWdBQmdBR0FBQUFBQUFDQUtELy8vOEFBQUVGRUFBQUFCd0FBQUFFQUFBQUFBQUFBQVFBQUFCdVlXMWxBQUFBQUFRQUJBQUVBQUFBelAvLy93QUFBUUlRQUFBQUhBQUFBQVFBQUFBQUFBQUFDQUFBQUhCc1lXNWxkRWxrQUFBQUFNVC8vLzhBQUFBQlFBQUFBQkFBRkFBSUFBWUFCd0FNQUFBQUVBQVFBQUFBQUFBQkFoQUFBQUFjQUFBQUJBQUFBQUFBQUFBQ0FBQUFhV1FBQUFnQURBQUlBQWNBQ0FBQUFBQUFBQUZBQUFBQQAYH3BhcnF1ZXQtY3BwLWFycm93IHZlcnNpb24gNS4wLjAZjBwAABwAABwAABwAABwAABwAABwAABwAAABdBgAAUEFSMQ=="
    )
    return pq.read_table(io.BytesIO(_decoded))


def schema():
    # fmt:off
    return RelationSchema(
            name="$satellites",
            columns=[
                FlatColumn(name="id", type=OrsoTypes.INTEGER),
                FlatColumn(name="planetId", type=OrsoTypes.INTEGER, aliases=["planet_id"]),
                FlatColumn(name="name", type=OrsoTypes.VARCHAR),
                FlatColumn(name="gm", type=OrsoTypes.DOUBLE),
                FlatColumn(name="radius", type=OrsoTypes.DOUBLE),
                FlatColumn(name="density", type=OrsoTypes.DOUBLE),
                FlatColumn(name="magnitude", type=OrsoTypes.DOUBLE),
                FlatColumn(name="albedo", type=OrsoTypes.DOUBLE),
            ],
        )


def statistics() -> RelationStatistics:
    stats = RelationStatistics()

    # fmt:off
    stats.record_count = 177
    stats.lower_bounds = {b'id': 1, b'radius': 0, b'planetId': 3, b'name': 4712016873010783585, b'magnitude': -13, b'gm': 0, b'albedo': 0, b'density': 0}
    stats.upper_bounds = {b'id': 177, b'radius': 2631, b'planetId': 9, b'name': 6443922580184236032, b'magnitude': 27, b'gm': 9888, b'albedo': 2, b'density': 4}
    stats.null_count = {b'id': 0, b'planetId': 0, b'name': 0, b'gm': 0, b'radius': 0, b'density': 0, b'magnitude': 0, b'albedo': 0}

    # fmt:on
    return stats
