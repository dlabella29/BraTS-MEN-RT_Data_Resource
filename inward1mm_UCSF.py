import nibabel as nib
import numpy as np
import os
import glob
from scipy.ndimage import binary_erosion


def erode_label(file_path, output_folder):
    # Load the NIfTI file
    img = nib.load(file_path)
    data = img.get_fdata()

    # Assume each voxel is 1mm; define a structuring element for 1mm erosion
    structure = np.ones((3, 3, 3))  # 3D erosion

    # Perform binary erosion
    eroded_data = binary_erosion(data > 0, structure=structure).astype(data.dtype)

    # Create a new NIfTI image using the modified data array
    new_img = nib.Nifti1Image(eroded_data, img.affine, img.header)

    # Prepare the output file path
    case_number = os.path.basename(file_path)[-14:-11]  # Extract the three digits from the filename
    output_file = os.path.join(output_folder, f'BraTS-MEN-UCSF-{case_number}_gtv-manual.nii.gz')

    # Save the new NIfTI file
    nib.save(new_img, output_file)
    print(f"File saved as {output_file}")


def process_directories(master_directory):
    # Iterate through all case folders in the master directory
    case_folders = glob.glob(os.path.join(master_directory, 'BraTS-MEN-UCSF-*'))
    for folder in case_folders:
        # Construct the file name based on the folder name
        case_number = os.path.basename(folder)[-3:]  # Extract the last three digits
        file_name = f'BraTS-MEN-UCSF-{case_number}_gtv.nii.gz'
        file_path = os.path.join(folder, file_name)

        if os.path.exists(file_path):
            erode_label(file_path, folder)
        else:
            print(f"No file found for {file_name} in {folder}")


# Master directory path
master_directory = r'C:\Users\User\Downloads\qc_pending-selected-UCSF'
process_directories(master_directory)
