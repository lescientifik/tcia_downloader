import argparse
import operator

parser = argparse.ArgumentParser("convert dicom files to nii.gz")
parser.add_argument("db", help="location of the csv create by the create_csv_db command")


# TODO

def group_by_correct_volumes(slices_mdatas):
    """Correctly group slices to obtain 3D volume.

    More specifically, the following thing can happened (have happened to me...):
    * different Acquisition number, but same volume
    * more to come...

    So, need to check both z-location (from ImagePositionPatient tag), instanceNumber and acquisition number
    to have a proper 3D volume
    """

    # start from a list of flat dict
    new_by_inst_nb = sorted(slices_mdatas, key=operator.itemgetter("AcquisitionNumber", "InstanceNumber"))
    new_by_zloc = sorted(slices_mdatas, key=operator.itemgetter("AcquisitionNumber", "InstanceNumber"))
    if not new_by_zloc == new_by_zloc:
        print("discrepancy in slice order between z position and instance number!")
        print("Using zloc to discriminate slice")