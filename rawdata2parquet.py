import os
import argparse

if __name__ == '__main__':
    # argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', help='filepath')
    args = parser.parse_args()

    # get filenames
    folders = os.listdir(args.path)

    for folder in folders:
        os.system('C:/nonBKU/python/python.exe D:/kmeans_earth_fault/createDayfile.py --path ' + args.path + '/' + folder)
