import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
import numpy
from opteryx.compiled.list_ops import list_ip_in_cidr

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

    # additional edge cases: Class A private ranges
    (["10.0.0.1", "10.255.255.255", "11.0.0.0"], "10.0.0.0/8", [True, True, False]),
    (["10.0.0.0", "10.127.255.255"], "10.0.0.0/9", [True, True]),
    (["10.128.0.0", "10.255.255.255"], "10.128.0.0/9", [True, True]),

    # additional edge cases: Class B private ranges
    (["172.16.0.0", "172.16.255.255", "172.15.255.255"], "172.16.0.0/16", [True, True, False]),
    (["172.20.0.1", "172.25.255.254"], "172.16.0.0/12", [True, True]),

    # additional edge cases: Class C private ranges
    (["192.168.0.1", "192.168.255.254", "192.169.0.1"], "192.168.0.0/16", [True, True, False]),
    (["192.168.100.0", "192.168.100.255"], "192.168.100.0/24", [True, True]),

    # additional edge cases: APIPA/Link-local addresses
    (["169.254.0.1", "169.254.255.254", "169.255.0.0"], "169.254.0.0/16", [True, True, False]),

    # additional edge cases: Multicast addresses
    (["224.0.0.0", "239.255.255.255", "240.0.0.0"], "224.0.0.0/4", [True, True, False]),

    # additional edge cases: Various subnet masks
    (["192.168.1.0", "192.168.1.15", "192.168.1.16"], "192.168.1.0/28", [True, True, False]),
    (["192.168.1.0", "192.168.1.31", "192.168.1.32"], "192.168.1.0/27", [True, True, False]),
    (["192.168.1.0", "192.168.1.63", "192.168.1.64"], "192.168.1.0/26", [True, True, False]),
    (["192.168.1.0", "192.168.1.127", "192.168.1.128"], "192.168.1.0/25", [True, True, False]),

    # additional edge cases: Single host masks
    (["8.8.8.8"], "8.8.8.8/32", [True]),
    (["8.8.8.8", "8.8.8.7"], "8.8.8.8/32", [True, False]),
    (["1.1.1.1"], "1.1.1.1/32", [True]),

    # additional edge cases: /30 subnets (point-to-point)
    (["192.168.1.0", "192.168.1.1", "192.168.1.2", "192.168.1.3"], "192.168.1.0/30", [True, True, True, True]),
    (["192.168.1.4"], "192.168.1.0/30", [False]),

    # additional edge cases: Public DNS servers
    (["8.8.8.8", "8.8.4.4"], "8.8.8.0/24", [True, False]),
    (["1.1.1.1", "1.0.0.1"], "1.0.0.0/8", [True, True]),

    # additional edge cases: Documentation/TEST-NET addresses
    (["192.0.2.0", "192.0.2.255", "192.0.3.0"], "192.0.2.0/24", [True, True, False]),
    (["198.51.100.0", "198.51.100.255"], "198.51.100.0/24", [True, True]),
    (["203.0.113.0", "203.0.113.255"], "203.0.113.0/24", [True, True]),

    # additional edge cases: Carrier-grade NAT
    (["100.64.0.0", "100.127.255.255", "100.128.0.0"], "100.64.0.0/10", [True, True, False]),

    # additional edge cases: Benchmark testing
    (["198.18.0.0", "198.19.255.255", "198.20.0.0"], "198.18.0.0/15", [True, True, False]),

    # additional edge cases: Reserved addresses
    (["240.0.0.0", "255.255.255.254"], "240.0.0.0/4", [True, True]),

    # additional edge cases: Leading zeros in IP (should be invalid if strict)
    (["192.168.001.1"], "192.168.1.0/24", [True]),  # leading zero
    (["192.168.1.01"], "192.168.1.0/24", [True]),  # leading zero

    # additional edge cases: Multiple IPs with same CIDR checks
    (["10.1.1.1", "10.2.2.2", "10.3.3.3", "10.4.4.4"], "10.0.0.0/8", [True, True, True, True]),
    (["172.31.0.1", "172.31.255.254"], "172.31.0.0/16", [True, True]),

    # additional edge cases: Consecutive subnets
    (["192.168.0.255", "192.168.1.0"], "192.168.0.0/24", [True, False]),
    (["192.168.1.0"], "192.168.0.0/23", [True]),

    # additional edge cases: Very small subnets
    (["192.168.1.2", "192.168.1.3", "192.168.1.4"], "192.168.1.2/31", [True, True]),
]


@pytest.mark.parametrize("ips, cidr, expected", TESTS)
def test_ip_containment(ips, cidr, expected):
    try:
        result = list_ip_in_cidr(numpy.array(ips), cidr)
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
