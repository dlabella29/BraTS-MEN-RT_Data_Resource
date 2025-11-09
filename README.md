# BraTS-MEN-RT_Data_Resource
This repository holds the scripts used to process the data found in the BraTS-MEN-RT dataset.

The UCSF -gtv.nii.gz cases all have a 1 voxel expansion added in the presegmentations. Therefore, "inward1mm_UCSF.py" creates a gtv-manual.nii.gz label file for the UCSF cases with a 1mm inward margin applied.

The PMH cases all have a translation of the -gtv.nii.gz label files of 1 voxel in "y" direction. Therefore, "translatePMH_y_plus_1.py" creates a gtv-manual.nii.gz label file with a translation of 1 voxel in -y direction.

The script extract_mri_params_to_xlsx.py creates a table of all of the relevant image parameter metadata for a specified institution's anonymized DICOM cases.
