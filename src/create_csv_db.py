""" https://github.com/pydicom/pydicom/issues/319#issuecomment-283003834
"""
import collections
from pathlib import Path

import pydicom as dicom
import pandas as pd
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("source", help="the root folder where to recursively search and analyse dicom filess")


def dicom_dataset_to_dict(dicom_header):
    dicom_dict = {}
    repr(dicom_header)
    for dicom_value in dicom_header.values():
        if dicom_value.tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        if type(dicom_value.value) == dicom.dataset.Dataset:
            dicom_dict[dicom_value.tag] = dicom_dataset_to_dict(dicom_value.value)
        else:
            v = _convert_value(dicom_value.value)
            dicom_dict[dicom_value.tag] = v
    return dicom_dict


def _sanitise_unicode(s):
    return s.replace(u"\u0000", "").strip()


def _convert_value(v):
    t = type(v)
    if t in (list, int, float):
        cv = v
    elif t == str:
        cv = _sanitise_unicode(v)
    elif t == bytes:
        s = v.decode('ascii', 'replace')
        cv = _sanitise_unicode(s)
    elif t == dicom.valuerep.DSfloat:
        cv = float(v)
    elif t == dicom.valuerep.IS:
        cv = int(v)
    elif t == dicom.valuerep.PersonName:
        cv = str(v)
    else:
        cv = repr(v)
    return cv


def flatten(d, parent_key='', sep='_'):
    """https://stackoverflow.com/a/6027615
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def extract_dcm_metadata_to_csv(folder: Path):
    folder = folder.expanduser().resolve()
    files = folder.rglob("*.dcm")
    list_of_metadata_dict = []
    for file in files:
        with dicom.dcmread(str(file), stop_before_pixels=True) as ds:
            list_of_metadata_dict.append(
                flatten(dicom_dataset_to_dict(ds))
            )
    df = pd.DataFrame.from_records(list_of_metadata_dict)
    df.to_xls(folder / "metadatas.xls", index=False)
    df.to_csv(folder / "metadatas.csv", index=False)

if __name__ == '__main__':
    args = parser.parse_args()
    extract_dcm_metadata_to_csv(Path(args.source))