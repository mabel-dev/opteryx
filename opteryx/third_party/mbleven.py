# cython: language_level=3
# Public domain
#
# I hereby place my work 'mbleven' into the public domain.
# All my copyrights, including all related and neighbouring
# rights, are abandoned.
#
# 2018 Fujimoto Seiji <fujimoto@ceptord.net>
# https://github.com/fujimotos/mbleven/blob/master/LICENSE

"""An implementation of mbleven algorithm"""

# This has been modified for Opteryx for performance.
# This is roughly twice as fast as the original implementation.
#
# This file maintains the Public Domain Licence, this does not change the licence
# of any other files in this project.

#
# Constants

REPLACE: int = 1
INSERT: int = 2
DELETE: int = 4

# fmt:off
MATRIX = (
    (
        (INSERT, DELETE, ),
        (DELETE, INSERT, ),
        (REPLACE, REPLACE, ),
    ),
    (
        (DELETE, REPLACE, ),
        (REPLACE, DELETE, ),
    ),
    (
        (DELETE, DELETE, ),
    )
)
# fmt:on

#
# Library API


def compare(str1: str, str2: str) -> int:
    len1, len2 = len(str1), len(str2)

    if len1 < len2:
        len1, len2 = len2, len1
        str1, str2 = str2, str1

    if len1 - len2 > 2:
        return -1

    models = MATRIX[len1 - len2]

    res = 3
    for model in models:
        cost = check_model(str1, str2, len1, len2, model)
        if cost < res:
            res = cost

    if res == 3:
        res = -1

    return res


def check_model(str1: str, str2: str, len1: int, len2: int, model) -> int:
    """Check if the model can transform str1 into str2"""

    idx1, idx2 = 0, 0
    cost, pad = 0, 0
    while (idx1 < len1) and (idx2 < len2):
        if str1[idx1] != str2[idx2 - pad]:
            cost += 1
            if 2 < cost:
                return cost

            option = model[cost - 1]
            if option == DELETE:
                idx1 += 1
            elif option == INSERT:
                idx2 += 1
            elif option == REPLACE:
                idx1 += 1
                idx2 += 1
                pad = 0
        else:
            idx1 += 1
            idx2 += 1
            pad = 0

    return cost + (len1 - idx1) + (len2 - idx2)
