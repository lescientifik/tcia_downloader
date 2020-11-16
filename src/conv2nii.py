import argparse

parser = argparse.ArgumentParser("convert dicom files to nii.gz")
parser.add_argument("db", help="location of the csv create by the create_csv_db command")

# TODO