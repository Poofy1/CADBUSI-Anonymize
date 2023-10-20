import pydicom
from PIL import Image
import numpy as np

def dicom_to_png(dicom_path, png_path):
    # Read the DICOM file
    ds = pydicom.dcmread(dicom_path)
    
    # Get the pixel array from the DICOM dataset
    pixel_array = ds.pixel_array

    # Normalize the pixel values to 8-bit (0-255)
    pixel_array = ((pixel_array - pixel_array.min()) / (pixel_array.max() - pixel_array.min()) * 255).astype(np.uint8)

    # Create a PIL Image from the NumPy array
    im = Image.fromarray(pixel_array, mode='L')  # 'L' specifies greyscale

    # Save the image
    im.save(png_path)

# Usage example
dicom_path = 'D:/DATA/CASBUSI/new_batch_(delete_me)/deidentified/image_07903059_12369630_1ca332dece14ba34baae174bf639f91acee055db.dcm'
png_path = 'D:/DATA/CASBUSI/new_batch_(delete_me)/test.png'

dicom_to_png(dicom_path, png_path)