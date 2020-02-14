import dicom2nifti


def convert_series(src, dest):
    dicom2nifti.dicom_series_to_nifti(src, dest, reorient_nifti=True)


if __name__ == "__main__":
    convert_series(
        "/home/theo/Téléchargements/anti_pd1_lung/test",
        "/home/theo/Téléchargements/anti_pd1_lung/test/doublet_test.nii.gz",
    )
