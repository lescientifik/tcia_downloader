import pydicom


def get_short_metadata(filename):
    field_names = (
        "StudyDate",
        "StudyTime",
        "Modality",
        "Manufacturer",
        "PatientName",
        "PatientID",
        "PatientSex",
        "PatientAge",
        "PatientSize",
        "PatientWeight",
        "ContrastBolusAgent",
        "SeriesNumber",
        "SliceLocation",
        "StudyInstanceUID",
        "SeriesInstanceUID",
        "SOPInstanceUID",
    )
    with pydicom.dcmread(filename, stop_before_pixels=True) as dcm:
        yield {
            value: (getattr(dcm, value) if hasattr(dcm, value) else None)
            for value in field_names
        }
