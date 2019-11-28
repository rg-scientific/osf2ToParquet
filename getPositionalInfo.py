#!/usr/bin/python
# encoding=utf8

import pandas as pd
import argparse
import os
from osf2 import read_osf2, get_channels_info


def createDataframes(df):
    features = ['gps_latitude', 'gps_longitude', 'gps_height']
    gps_longitude = df[df['channel'] == features[0]]
    gps_latitude = df[df['channel'] == features[1]]
    gps_height = df[df['channel'] == features[2]]
    
    return gps_latitude, gps_longitude, gps_height

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
    
    gps_latitude, gps_longitude, gps_height = createDataframes(result)

    return gps_latitude, gps_longitude, gps_height

def createDayframes(path, files):
    filepaths = [(path + '/' + item) for item in files]

    gps_latitude_list = []
    gps_longitude_list = []
    gps_height_list = []

    for filepath in filepaths:
        gps_latitude, gps_longitude, gps_height = createFeatureframesFromOsf2(filepath)
        gps_latitude_list.append(gps_latitude)
        gps_longitude_list.append(gps_longitude)
        gps_height_list.append(gps_height)


    gps_latitude_merged = pd.concat(gps_latitude_list).sort_values(by='utc', ascending=True)
    gps_longitude_merged = pd.concat(gps_longitude_list).sort_values(by='utc', ascending=True)
    gps_height_merged = pd.concat(gps_height_list).sort_values(by='utc', ascending=True)


    return gps_latitude_merged, gps_longitude_merged, gps_height_merged

def resampleMergeAndSaveAsCSV(gps_latitude_merged, gps_longitude_merged, gps_height_merged):
    #objs1 = [Min_01_merged, Max_01_merged, RMS_01_merged, Min_02_merged, Max_02_merged, RMS_02_merged]

    gps_latitude_ts = gps_latitude_merged.set_index('utc')
    gps_longitude_ts = gps_longitude_merged.set_index('utc')
    gps_height_ts = gps_height_merged.set_index('utc')

    gps_latitude_ts = gps_latitude_ts[~gps_latitude_ts.index.duplicated(keep='first')]
    gps_longitude_ts = gps_longitude_ts[~gps_longitude_ts.index.duplicated(keep='first')]
    gps_height_ts = gps_height_ts[~gps_height_ts.index.duplicated(keep='first')]

    gps_latitude_ts = gps_latitude_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    gps_longitude_ts = gps_longitude_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)
    gps_height_ts = gps_height_ts.resample('1S').pad().fillna(method='backfill').drop('channel', axis=1)

    gps_latitude_ts.columns = ['gps_latitude']
    gps_longitude_ts.columns = ['gps_longitude']
    gps_height_ts.columns = ['gps_height']



    objs = [gps_latitude_ts, gps_longitude_ts, gps_height_ts]
    merged =pd.concat(objs, axis=1, join='outer', ignore_index=False).interpolate(method='linear', limit_direction ='both')
    merged.reset_index(drop=False, inplace=True)
    merged.rename(columns={'utc': 'time'}, inplace=True)
    print(merged.head())
    merged.to_csv('072_20190630_gps')

if __name__ == '__main__':
    # argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--inpath', help='inpath')
    parser.add_argument('--outpath', help='outpath')
    args = parser.parse_args()

    # get filenames
    files = os.listdir(args.inpath)
    files_osf = [i for i in files if i.endswith('.osf')]
    fnoutfile = args.inpath.split('/')#[-1]
    #print(fnoutfile)
    #outpath = '/'.join(map(str, str(args.path).split('/')[:-1]))
    gps_latitude_merged, gps_longitude_merged, gps_height_merged = createDayframes(args.inpath, files_osf)
    resampleMergeAndSaveAsCSV(gps_latitude_merged, gps_longitude_merged, gps_height_merged)