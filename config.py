# Global Setting for the Database
# PageSize, StartRID, etc..
import struct


def init():
    pass

def encode(value):
    return struct.pack(ENCODING, value)


def decode(value):
    return struct.unpack(ENCODING, value)[0]


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

PAGE_SIZE = 4096
RECORDS_PER_PAGE = PAGE_SIZE / 8
ENCODING = ">Q"
PAGE_RANGE_SIZE = 10000
LID_MAX = (2 ** 64)
ENCODED_ZERO = encode(0)
NUMBER_OF_META_COLUMNS = 4
