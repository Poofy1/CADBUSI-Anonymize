import os
import pydicom
from PIL import Image
import numpy as np


def process_file(dicom_path, png_path):
    # Read the DICOM file
    ds = pydicom.dcmread(dicom_path)

    #print(ds)

    # Get the pixel array from the DICOM dataset
    pixel_array = ds.pixel_array

    # Normalize the pixel values to 8-bit (0-255), if necessary
    if pixel_array.dtype != np.uint8:
        pixel_array = ((pixel_array - pixel_array.min()) / (pixel_array.max() - pixel_array.min()) * 255).astype(np.uint8)

    # Check the number of dimensions in the array
    if len(pixel_array.shape) == 2:
        # Grayscale image (2D array)
        mode = 'L'
    elif len(pixel_array.shape) == 3:
        # RGB image (3D array)
        mode = 'RGB'
    else:
        raise ValueError('Unsupported number of dimensions in pixel array')

    # Create a PIL Image from the NumPy array
    im = Image.fromarray(pixel_array, mode=mode)

    # Save the image
    im.save(png_path)



def dicom_to_png(input_path, output_folder):

    os.makedirs(output_folder, exist_ok=True)    
    # Check if input path is a directory
    if os.path.isdir(input_path):
        # List all files in the directory
        for root, dirs, files in os.walk(input_path):
            for file in files:
                # Check if the file is a DICOM file
                if file.lower().endswith('.dcm'):
                    # Construct the full DICOM file path
                    dicom_path = os.path.join(root, file)
                    # Construct the PNG file path
                    png_path = os.path.join(output_folder, f"{os.path.splitext(file)[0]}.png")
                    # Process the DICOM file
                    process_file(dicom_path, png_path)
    else:
        # Construct the PNG file path from the DICOM file name
        file_name = os.path.basename(input_path)
        png_path = os.path.join(output_folder, f"{os.path.splitext(file_name)[0]}.png")
        # Process the single DICOM file
        process_file(input_path, png_path)


# Example usage:
input_path = 'D:/DATA/CASBUSI/new_batch_(delete_me)/deidentified'
output_folder = 'D:/DATA/CASBUSI/new_batch_(delete_me)/test_out'

dicom_to_png(input_path, output_folder)
