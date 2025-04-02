import pydicom
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import random

def display_dicom(dicom_file_path):
    try:
        # Load the DICOM file
        ds = pydicom.dcmread(dicom_file_path, force=True)
        
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
            pixel_array = ds.pixel_array
            
            # Check if this is a multi-frame (video) DICOM
            if len(pixel_array.shape) > 2:
                print(f"Multi-frame DICOM detected with shape: {pixel_array.shape}")
                
                # For multi-frame data, select a random frame
                num_frames = pixel_array.shape[0]
                random_frame_idx = random.randint(0, num_frames - 1)
                print(f"Displaying frame {random_frame_idx} of {num_frames}")
                
                # Extract the selected frame
                frame = pixel_array[random_frame_idx]
                
                # Check if the frame has color channels (shape with length > 2)
                if len(frame.shape) > 2:
                    plt.figure(figsize=(10, 8))
                    plt.imshow(frame)  # For RGB data
                else:
                    plt.figure(figsize=(10, 8))
                    plt.imshow(frame, cmap=plt.cm.bone)  # For grayscale data
                
                plt.title(f"DICOM Video - Frame {random_frame_idx}/{num_frames}")
            else:
                # For single-frame images
                plt.figure(figsize=(10, 8))
                plt.imshow(pixel_array, cmap=plt.cm.bone)
                plt.title(f"DICOM Image: {os.path.basename(dicom_file_path)}")
            
            plt.axis('off')
            plt.tight_layout()
            plt.show()
        else:
            print("This DICOM file does not contain image data.")
            
    except Exception as e:
        print(f"Error processing DICOM file: {e}")

if __name__ == "__main__":
    display_dicom("F:/CODE/CADBUSI/CADBUSI-Anonymize/dicoms/anon_dicoms_2025-04-01_221610_07754096_5482175_video_07754096_05482175_2f18862c0d2740cb581a910e47d1a9dd90ebf9890264102d05b3b98b7e8ec093.dcm")