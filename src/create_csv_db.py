""" https://github.com/pydicom/pydicom/issues/319#issuecomment-283003834
"""
import argparse
import collections
from collections.abc import MutableMapping
from pathlib import Path
from typing import List, Dict

import pandas as pd
import pydicom as dicom
from joblib import Parallel, delayed

from src.dicom_keys import DICOM_TAGS_TO_KEEP
from src.filters import keep_slice, small_series

parser = argparse.ArgumentParser()
parser.add_argument("source", help="the root folder where to recursively search and analyse dicom filess")
parser.add_argument("--jobs", "-j", help="Number of workers to use", default=4, type=int)
parser.add_argument("--filter_small_series", help="filter series with less than 25 slices in it", action="store_true")
parser.add_argument("--filter_slices", help="keep only CT,MR,AC PT,RTSTRUC and SEG, original acquisition only",
                    action="store_true")

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
        # add a z_location key for later use
        if hasattr(dicom_dict, "ImagePositionPatient"):
            dicom_dict["z_location"] = dicom_header.ImagePositionPatient[-1]
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


def dcm_file_to_flat_dict(file):
    print(f"Working on {file}")
    with dicom.dcmread(str(file), stop_before_pixels=True) as ds:
        extract = dicom_dataset_to_flat_dict(ds)
        m_datas = {key: value for key, value in extract.items() if key in DICOM_TAGS_TO_KEEP}
        m_datas["file_location"] = str(file.resolve())
    return m_datas


def merge_series(list_of_metas: List[Dict]) -> Dict:
    """Merge series with the same SeriesUID.
    """
    result = collections.defaultdict(list)
    for metas in list_of_metas:
            result[metas["SeriesInstanceUID"]].append(metas)
    return result


def extract_dcm_metadata_to_csv(folder: Path, n_jobs, filter_slice=True, filter_series=True):
    folder = folder.expanduser().resolve()
    files = folder.rglob("*.dcm")
    with Parallel(n_jobs=n_jobs) as parallel:
        list_of_metadata_dict = parallel(delayed(dcm_file_to_flat_dict)(file) for file in files)
        if filter_slice:
            indexer = parallel(delayed(keep_slice)(slice_) for slice_ in list_of_metadata_dict)
            list_of_metadata_dict = [x for x, y in zip(list_of_metadata_dict, indexer) if y]
    metadatas_group_by_series_acq_number = merge_series(list_of_metadata_dict)
    final_list_of_mdatas = []
    for unique_series, series_slices in metadatas_group_by_series_acq_number.items():
        if filter_series and small_series(series_slices):
            continue
        else:
            final_list_of_mdatas.extend(series_slices)
    df = pd.DataFrame.from_records(final_list_of_mdatas)
    df.to_csv(folder / "metadatas.csv", index=False)


if __name__ == '__main__':
    args = parser.parse_args()
    print(args)
    extract_dcm_metadata_to_csv(Path(args.source), args.jobs, args.filter_slices, args.filter_small_series)
