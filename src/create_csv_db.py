""" https://github.com/pydicom/pydicom/issues/319#issuecomment-283003834
"""
import argparse
from collections.abc import MutableMapping
from pathlib import Path

import pandas as pd
import pydicom as dicom
from joblib import Parallel, delayed

parser = argparse.ArgumentParser()
parser.add_argument("source", help="the root folder where to recursively search and analyse dicom filess")
parser.add_argument("--jobs", "-j", help="Number of workers to use", default=4, type=int)

dicom.config.datetime_conversion = True


def dicom_dataset_to_flat_dict(dicom_header):
    dicom_dict = {}
    repr(dicom_header)
    for dicom_value in dicom_header.values():
        if dicom_value.tag == (0x7fe0, 0x0010):
            # discard pixel data
            continue
        if isinstance(dicom_value.value, dicom.dataset.Dataset):
            dicom_dict[dicom_value.keyword] = dicom_dataset_to_flat_dict(dicom_value.value)
        elif isinstance(dicom_value.value, dicom.sequence.Sequence):
            for i, dataset in enumerate(dicom_value.value):
                dicom_dict[dicom_value.keyword + str(i)] = dicom_dataset_to_flat_dict(dataset)
        else:
            v = _convert_value(dicom_value.value)
            dicom_dict[dicom_value.keyword] = v
    return flatten(dicom_dict)


def _sanitise_unicode(s):
    return s.replace(u"\u0000", "").strip().rstrip("\n")


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
    elif t in (dicom.valuerep.DA, dicom.valuerep.TM, dicom.valuerep.DT):
        cv = v.isoformat()
    else:
        cv = repr(v)
    return cv


def flatten(d, parent_key='', sep='_'):
    """https://stackoverflow.com/a/6027615
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def extract_dcm_metadata_to_csv(folder: Path, n_jobs):
    folder = folder.expanduser().resolve()
    files = folder.rglob("*.dcm")
    if n_jobs == 1:
        list_of_metadata_dict = [dcm_file_to_flat_dict(file) for file in files]
    else:
        list_of_metadata_dict = Parallel(n_jobs=n_jobs)(delayed(dcm_file_to_flat_dict)(file) for file in files)
    df = pd.DataFrame.from_records(list_of_metadata_dict)
    print(df.convert_dtypes().dtypes)
    print(df.PatientWeight.dtype)
    df.to_csv(folder / "metadatas.csv", index=False)


def dcm_file_to_flat_dict(file):
    print(f"Working on {file}")
    with dicom.dcmread(str(file), stop_before_pixels=True) as ds:
        extract = flatten(dicom_dataset_to_flat_dict(ds))
        extract["file_location"] = str(file.resolve())
    return extract


if __name__ == '__main__':
    args = parser.parse_args()
    extract_dcm_metadata_to_csv(Path(args.source), args.jobs)
