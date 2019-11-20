from xml.etree import cElementTree as xmlTree
from datetime import datetime
import struct
import numpy as np
import pandas as pd
import io

def read_osf2(binary):
    # Checking that the file starts with b'OCEAN_STREAM_FORMAT2'
    if binary.find(b'OCEAN_STREAM_FORMAT2') != 0:
        raise Exception('Unexpected format')

    # Find the ending of the xml header and separate it from the binary content
    header_start = binary.index(b'\n') + 1
    header_end = binary.index(b'</ocean>\n') + len(b'</ocean>\n')
    header_str = binary[header_start:header_end]

    header = xmlTree.fromstring(header_str)
    content = parse_content(binary[header_end:])

    return (header, content)


def get_creation_time(header):
    utc = int(header.get('created_utc'))
    dt = datetime.utcfromtimestamp(utc / 1000.0)
    return dt


def get_channels_info(header):
    channels = np.array([
        [channel.get('index'), channel.get('physicalunit'), *channel.get('oid').split('/')]
        for channel in header.getchildren()[0]
    ])

    index = pd.Index(channels[:, 0].astype(np.uint), name='id')

    df = pd.DataFrame(channels[:, 1:], columns=['physical_unit', 'message', 'signal'], index=index) # , 'physical_dimension', 'type'

    return df

def parse_content(content):
    data = []

    cache = {}

    with io.BytesIO(content) as fstream:
        channel = None

        while True:
            buf = fstream.read(1)
            if not buf:
                # No data left in the stream
                break
            # B == unsigned char
            flag = struct.unpack('B', buf)[0]

            try:
                # Channel id (uint16)
                if flag & 1:
                    channel = struct.unpack('H', fstream.read(2))[0]
                    if channel not in cache:
                        cache[channel] = { 'group': None, 'utc': None }

                # Unix timestamp (ms) / delta since the last record within the same channel (uint64 / uint8)
                if flag & 2:
                    utc = struct.unpack('Q', fstream.read(8))[0]
                    delta_ts = None
                    cache[channel]['group'] = utc
                    cache[channel]['utc'] = utc
                else:
                    delta_ts = struct.unpack('B', fstream.read(1))[0]
                    
                    if cache[channel]['utc'] is None:
                        raise Exception('Timestamp not initialized for channel {}'.format(channel))
                    else:
                        cache[channel]['utc'] += delta_ts

                # Channel status (uint32)
                if flag & 4:
                    status = struct.unpack('I', fstream.read(4))[0]
                else:
                    status = None

                # Sample value (double)
                if flag & 8:
                    value = struct.unpack('d', fstream.read(8))[0]
                else:
                    value = None

            except struct.error:
                print("Unexpected EOF")
                break

            data.append([channel, cache[channel]['group'], cache[channel]['utc'], status, value])

    df = pd.DataFrame(data, columns=['channel', 'group', 'utc', 'status', 'value'])
    return df
