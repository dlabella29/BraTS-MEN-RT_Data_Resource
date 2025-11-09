import nibabel as nib
import numpy as np
import os
import glob


def shift_y_coordinate(file_path, output_folder):
    # Load the NIfTI file
    img = nib.load(file_path)
    data = img.get_fdata()

    # Create an empty array with the same shape as the original data
    new_data = np.zeros(data.shape, dtype=data.dtype)

    # Increase the y-coordinate of labeled pixels by 1
    for x in range(data.shape[0]):
        for z in range(data.shape[2]):
            for y in range(1, data.shape[1]):  # Start from 1 to avoid index out of bounds
                if data[x, y, z] > 0:
                    new_data[x, y , z] = data[x, y, z]

    # Create a new NIfTI image using the modified data array
    new_img = nib.Nifti1Image(new_data, img.affine, img.header)

    # Prepare the output file path
    output_file = os.path.join(output_folder, 'BraTS-MEN-PMH-' + file_path[-14:-7] + '-manual.nii.gz')

    # Save the new NIfTI file
    nib.save(new_img, output_file)
    print(f"File saved as {output_file}")


def process_directories(master_directory):
    # Iterate through all case folders in the master directory
    case_folders = glob.glob(os.path.join(master_directory, 'BraTS-MEN-PMH-*'))
    for folder in case_folders:
        # Construct the file name based on the folder name
        case_number = os.path.basename(folder)[-3:]  # Extract the last three digits
        file_name = f'BraTS-MEN-PMH-{case_number}_gtv.nii.gz'
        file_path = os.path.join(folder, file_name)

        if os.path.exists(file_path):
            shift_y_coordinate(file_path, folder)
        else:
            print(f"No file found for {file_name} in {folder}")


# Master directory path
master_directory = r'C:\Users\User\Downloads\qc_pending-selected-PMH'
process_directories(master_directory)
