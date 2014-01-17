import struct


def decode_text(string):
    """
    decode bytestring as utf-8
    """
    return string.decode('utf-8')

def len_unpacker(val):
    return struct.Struct('>H').unpack(val)[0]

def unpack(bytestr):
    # The composite format for each component is:
    #   <len>   <value>   <eoc>
    # 2 bytes | ? bytes | 1 byte
    components = []
    while bytestr:
        length = len_unpacker(bytestr[:2])
        components.append(decode_text(bytestr[2:2 + length]))
        bytestr = bytestr[3 + length:]
    return tuple(components)

def decode(item):
    """
    decode a query result consisting of user types
    
    returns nested arrays representing user type ordering
    """
    decoded = []
       
    if isinstance(item, tuple) or isinstance(item, list):
        for i in item:
            decoded.extend(decode(i))
    else:
        if item.startswith('\x00'):
            unpacked = unpack(item)
            decoded.append(decode(unpacked))
        else:
            decoded.append(item)
    
    return decoded