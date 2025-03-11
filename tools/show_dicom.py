import pydicom
import matplotlib.pyplot as plt
import sys
import os

def display_dicom(dicom_file_path):
    try:
        # Load the DICOM file
        ds = pydicom.dcmread(dicom_file_path)
        
        # Print all metadata
        print("DICOM Metadata:")
        print("=" * 50)
        for elem in ds:
            if elem.VR != "SQ":  # Skip sequence items to avoid excessive output
                try:
                    if elem.name != "Pixel Data":
                        print(f"{elem.name}: {elem.value}")
                except:
                    print(f"{elem.tag}: Unable to display value")
        
        # Display the image
        print("\nDisplaying image...")
        
        # Check if the file has pixel data
        if hasattr(ds, 'pixel_array'):
            plt.figure(figsize=(10, 8))
            plt.imshow(ds.pixel_array, cmap=plt.cm.bone)
            plt.title(f"DICOM Image: {os.path.basename(dicom_file_path)}")
            plt.axis('off')
            plt.tight_layout()
            plt.show()
        else:
            print("This DICOM file does not contain image data.")
            
    except Exception as e:
        print(f"Error processing DICOM file: {e}")

if __name__ == "__main__":
    
    display_dicom("F:/CODE/CADBUSI/CADBUSI-Anonymize/anonymized/image_02263078_68896714-2_bc5b0224c104c4a3fd7cd8868d37b9eb75b3cfe1d95fcc0c62b2c42555f69f45.dcm")