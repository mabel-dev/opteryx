import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest
import numpy
from opteryx.compiled.functions.ip_address import ip_in_cidr

TESTS = [
    # Test case 1: Single IP in CIDR
    (["192.168.1.1"], "192.168.1.0/24", [True]),

    # Test case 2: Single IP not in CIDR
    (["192.168.2.1"], "192.168.1.0/24", [False]),

    # Test case 3: Multiple IPs, some in CIDR, some not
    (["192.168.1.1", "192.168.1.255", "192.168.2.1"], "192.168.1.0/24", [True, True, False]),

    # Test case 4: All IPs in CIDR
    (["10.0.0.1", "10.0.0.2", "10.0.0.3"], "10.0.0.0/8", [True, True, True]),

    # Test case 5: No IPs in CIDR
    (["172.16.0.1", "172.16.0.2", "172.16.0.3"], "192.168.0.0/16", [False, False, False]),

    # Test case 6: Edge case - CIDR with /32 mask
    (["192.168.1.1", "192.168.1.2"], "192.168.1.1/32", [True, False]),

    # Test case 7: Edge case - CIDR with /0 mask (all IPs should match)
    (["8.8.8.8", "1.1.1.1"], "0.0.0.0/0", [True, True]),

    # Test case 8: Invalid IP address in list
    (["192.168.1.1", "invalid_ip"], "192.168.1.0/24", ValueError),

    # Test case 9: Empty IP list
    ([], "192.168.1.0/24", []),

    # Test case 10: Invalid CIDR notation
    (["192.168.1.1"], "192.168.1.0/33", ValueError),

    # Test case 11: Boundary IPs at the edges of a /24
    (["192.168.1.0", "192.168.1.255"], "192.168.1.0/24", [True, True]),

    # Test case 12: Checking a /31 boundary
    (["192.168.1.0", "192.168.1.1", "192.168.1.2"], "192.168.1.0/31", [True, True, False]),

    # Test case 13: IP in a broader network vs. out of range
    (["172.16.200.10", "192.168.1.1"], "172.16.0.0/12", [True, False]),

    # Test case 14: Invalid IP (octet out of range)
    (["192.168.1.300"], "192.168.1.0/24", ValueError),

    # Test case 15: Malformed IP (missing octet)
    (["192.168.1"], "192.168.1.0/24", ValueError),

    # Test case 16: Negative mask
    (["192.168.1.1"], "192.168.1.0/-1", ValueError),

    # Test case 17: All-zero network with /0 mask (all IPs should match)
    (["1.1.1.1", "255.255.255.255", "192.168.1.1"], "0.0.0.0/0", [True, True, True]),

    # Test case 18: All-zero network with /32 mask (only exact 0.0.0.0 should match)
    (["0.0.0.0", "1.1.1.1", "192.168.1.1"], "0.0.0.0/32", [True, False, False]),

    # Test case 19: Uncommon mask /27 (32 IPs range)
    (["192.168.1.1", "192.168.1.31", "192.168.1.32"], "192.168.1.0/27", [True, True, False]),

    # Test case 20: Uncommon mask /29 (8 IPs range)
    (["192.168.1.1", "192.168.1.7", "192.168.1.8"], "192.168.1.0/29", [True, True, False]),

    # Test case 21: Uncommon mask /31 (2 IPs range, for point-to-point links)
    (["192.168.1.0", "192.168.1.1", "192.168.1.2"], "192.168.1.0/31", [True, True, False]),

    # Test case 22: Boundary IPs for /16
    (["192.168.0.0", "192.168.255.255", "193.0.0.0"], "192.168.0.0/16", [True, True, False]),

    # Test case 23: Network and broadcast addresses for /24
    (["192.168.1.0", "192.168.1.255", "192.168.2.0"], "192.168.1.0/24", [True, True, False]),

    # Test case 24: Network and broadcast addresses for /29
    (["192.168.1.0", "192.168.1.7", "192.168.1.8"], "192.168.1.0/29", [True, True, False]),

    # Test case 25: Mask with single IP /32 (only exact match)
    (["10.0.0.1", "10.0.0.2"], "10.0.0.1/32", [True, False]),

    # Test case 26: IPv4 loopback address with /8 mask
    (["127.0.0.1", "127.255.255.255", "128.0.0.1"], "127.0.0.0/8", [True, True, False]),

    # Test case 27: Private IP range for /12 (172.16.0.0 to 172.31.255.255)
    (["172.16.0.1", "172.31.255.255", "172.32.0.0"], "172.16.0.0/12", [True, True, False]),

    # Test case 28: Invalid CIDR notation - missing mask
    (["192.168.1.1"], "192.168.1.0", ValueError),

    # Test case 29: Invalid IP address - out of range octet
    (["300.168.1.1"], "192.168.1.0/24", ValueError),

    # Test case 30: Malformed CIDR - extra characters
    (["192.168.1.1"], "192.168.1.0/24abc", ValueError),
]


@pytest.mark.parametrize("ips, cidr, expected", TESTS)
def test_ip_containment(ips, cidr, expected):
    try:
        result = ip_in_cidr(numpy.array(ips), cidr)
        assert (x == y for x, y in zip(result, expected))
    except AssertionError as e:
        assert False, (ips, cidr, expected, result)
    except Exception as e:
        assert expected == type(e), (ips, cidr, expected, type(e))
        


if __name__ == "__main__":  # pragma: no cover
    print(f"RUNNING BATTERY OF {len(TESTS)} IP CONTAINMENT TESTS")
    for ips, cidr, expected in TESTS:
        test_ip_containment(ips, cidr, expected)
        print("\033[38;2;26;185;67m.\033[0m", end="")

    print()
    print("✅ okay")
