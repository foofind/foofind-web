# -*- coding: utf-8 -*-

from foofind.utils import logging
import socket, struct

class IPRanges:
    '''
    Allow to check an ip for a set of ranges.
    '''
    IP_PACKER = struct.Struct("!L")

    def __init__(self):
        self.N = 0      # maximum number of bits that are not part of any range mask
        self.ips = {}   # masks grouped by first 32-N bits

    def load(self, filename):
        '''
        Load ip ranges from a file. This must have one line per range in format IP/RANGE_SIZE
        '''
        try:
            # read file
            with open(filename, 'r') as f:
                ip_ranges = dict(self.parse_ip_range(ip_range) for ip_range in f if ip_range)

            # calculates the value for N
            self.N = 32-min(ip_ranges.itervalues())

            # create masks groups
            for range_ip, range_mask_size in ip_ranges.iteritems():
                masks_key = range_ip>>self.N
                if masks_key not in self.ips:
                    self.ips[masks_key] = []
                range_mask = ((1L<<range_mask_size)-1)<<(32-range_mask_size)
                self.ips[masks_key].append((range_ip&range_mask, range_mask))
        except BaseException as e:
            logging.exception(e)

    def _ip2long(self, ip):
        '''
        Convert an IP in string format to an integer.
        '''
        return self.IP_PACKER.unpack(socket.inet_aton(ip))[0]

    def parse_ip_range(self, ip_range):
        '''
        Convert an IP/RANGE string to a tuple with an integer and a mask.
        '''
        ip, range_mask_size = ip_range.split("/")
        return self._ip2long(ip), int(range_mask_size)

    def __contains__(self, ip):
        '''
        Checks if the ip is included in any IP range.
        '''
        iplong = self._ip2long(ip)
        ranges = self.ips.get(iplong>>self.N, None)
        return ranges and any(iplong&range_mask==range_ip for range_ip, range_mask in ranges) or False
