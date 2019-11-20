import os
import argparse

if __name__ == '__main__':
    # argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--inpath', help='inpath')
    parser.add_argument('--outpath', help='outpath')
    args = parser.parse_args()

    # get filenames
    folders = os.listdir(args.inpath)

    for folder in folders:
        os.system('python3 createDayfile.py --inpath ' + ''.join([args.inpath, folder]) + ' --outpath ' + args.outpath)
