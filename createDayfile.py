#!/usr/bin/python
# encoding=utf8

import pandas as pd
import argparse
import os
from osf2 import read_osf2, get_channels_info


def createDataframes(df):
    features = ['Min_01', 'Max_01', 'RMS_01', 'Min_02', 'Max_02', 'RMS_02']
    Min_01 = df[df['channel'] == features[0]]
    Max_01 = df[df['channel'] == features[1]]
    RMS_01 = df[df['channel'] == features[2]]
    Min_02 = df[df['channel'] == features[3]]
    Max_02 = df[df['channel'] == features[4]]
    RMS_02 = df[df['channel'] == features[5]]

    return Min_01, Max_01, RMS_01, Min_02, Max_02, RMS_02

def createFeatureframesFromOsf2(osf):
    (header, content) = read_osf2(open(osf, mode='rb').read())
    #write channels 2 dictionary
    channels = get_channels_info(header)
    channels['id'] = channels.index
    channeldict = {}
    for i in range(0, len(channels)):
        channeldict.update({channels['id'][i]:channels['signal'][i]})

    content['channel'] = content['channel'].replace(to_replace=channeldict)
    content['utc'] = content['utc'] / 1000
    content['utc'] = pd.to_datetime(content['utc'], unit='s')

    result = pd.DataFrame(content[['channel', 'utc', 'value']])

    Min_01, Max_01, RMS_01, Min_02, Max_02, RMS_02 = createDataframes(result)

    '''
    content = content.set_index('channel', append=True).sort_index(level=1).reset_index(level=1)

    result = {}

    # Restructuring the resulting data frame to make it database-friendly
    for message, channel in channels.groupby('message', sort=False):
        # Selecting data points from the current group
        data = content[content.channel.isin(channel.index)]

        # Rotating the data frame:
        #   - filtering out the NaN values
        #   - channel id becomes a column index instead of a row index
        data = data[data.value.notna()].pivot(index='utc', columns='channel', values=['status', 'value'])

        # Replacing channel id with channel name in the column indices
        data.columns = ['{}_{}'.format(key, channel.loc[id]['signal']) for key, id in data.columns.values]

        # Moving timestamp from the row index back to a regular column
        data.reset_index(inplace=True)

        # Saving the data frame, if there's anything to save
        if len(data):
            result[message] = data
    '''
    return Min_01, Max_01, RMS_01, Min_02, Max_02, RMS_02

def createDayframes(path, files):
    filepaths = [(path + '/' + item) for item in files]

    Min_01_list = []
    Max_01_list = []
    RMS_01_list = []
    Min_02_list = []
    Max_02_list = []
    RMS_02_list = []

    for filepath in filepaths:
        Min_01, Max_01, RMS_01, Min_02, Max_02, RMS_02 = createFeatureframesFromOsf2(filepath)
        Min_01_list.append(Min_01)
        Max_01_list.append(Max_01)
        RMS_01_list.append(RMS_01)
        Min_02_list.append(Min_02)
        Max_02_list.append(Max_02)
        RMS_02_list.append(RMS_02)

    Min_01_merged = pd.concat(Min_01_list).sort_values(by='utc', ascending=True)
    Max_01_merged = pd.concat(Max_01_list).sort_values(by='utc', ascending=True)
    RMS_01_merged = pd.concat(RMS_01_list).sort_values(by='utc', ascending=True)
    Min_02_merged = pd.concat(Min_02_list).sort_values(by='utc', ascending=True)
    Max_02_merged = pd.concat(Max_02_list).sort_values(by='utc', ascending=True)
    RMS_02_merged = pd.concat(RMS_02_list).sort_values(by='utc', ascending=True)

    return Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged

def resampleMergeAndSaveAsParquet(Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged):
    objs1 = [Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged]

    Min_01_ts = Min_01_merged.set_index('utc')
    Max_01_ts = Max_01_merged.set_index('utc')
    RMS_01_ts = RMS_01_merged.set_index('utc')
    Min_02_ts = Min_02_merged.set_index('utc')
    Max_02_ts = Max_02_merged.set_index('utc')
    RMS_02_ts = RMS_02_merged.set_index('utc')

    Min_01_ts = Min_01_ts[~Min_01_ts.index.duplicated(keep='first')]
    Max_01_ts = Max_01_ts[~Max_01_ts.index.duplicated(keep='first')]
    RMS_01_ts = RMS_01_ts[~RMS_01_ts.index.duplicated(keep='first')]
    Min_02_ts = Min_02_ts[~Min_02_ts.index.duplicated(keep='first')]
    Max_02_ts = Max_02_ts[~Max_02_ts.index.duplicated(keep='first')]
    RMS_02_ts = RMS_02_ts[~RMS_02_ts.index.duplicated(keep='first')]

    Min_01_ts = Min_01_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    Max_01_ts = Max_01_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    RMS_01_ts = RMS_01_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    Min_02_ts = Min_02_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    Max_02_ts = Max_02_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    RMS_02_ts = RMS_02_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)

    Min_01_ts.columns = ['Min_01']
    Max_01_ts.columns = ['Max_01']
    RMS_01_ts.columns = ['RMS_01']
    Min_02_ts.columns = ['Min_02']
    Max_02_ts.columns = ['Max_02']
    RMS_02_ts.columns = ['RMS_02']


    objs = [Min_01_ts, Max_01_ts, RMS_01_ts, Min_02_ts, Max_02_ts, RMS_02_ts]
    merged =pd.concat(objs, axis=1, join='outer', ignore_index=False).interpolate(method='linear', limit_direction ='both')
    merged.reset_index(drop=False, inplace=True)
    merged.rename(columns={'utc': 'time'}, inplace=True)
    merged.to_parquet(outpath + '/' + fnoutfile + '.parquet', engine='pyarrow')

if __name__ == '__main__':
    # argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='filepath')
    args = parser.parse_args()

    # get filenames
    files = os.listdir(args.path)
    files_osf = [i for i in files if i.endswith('.osf')]
    fnoutfile = str(args.path).split('/')[-1]
    outpath = '/'.join(map(str, str(args.path).split('/')[:-1]))
    Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged = createDayframes(args.path, files_osf)
    resampleMergeAndSaveAsParquet(Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged)

