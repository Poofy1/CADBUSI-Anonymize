from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os
import csv
import pickle
import struct

def generate_key():
    return os.urandom(16)  # 128-bit key


def get_encryption_key():
    """
    Retrieves or generates an encryption key for DICOM deidentification.
    
    The function looks for an existing key in a predefined location or environment variable.
    If no key exists, it generates a new one and stores it.
    
    Returns:
        bytes: A 16-byte encryption key for use with AES-128
    """
    # Check for key in environment variable first
    import os
    env_key_name = "DICOM_ENCRYPTION_KEY"
    key_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "encryption_key.pkl")
    
    # Try to get key from environment variable (base64 encoded)
    import base64
    if env_key_name in os.environ:
        try:
            return base64.b64decode(os.environ[env_key_name])
        except Exception as e:
            print(f"Error loading key from environment: {e}")
    
    # If not in environment, try to load from file
    import pickle
    if os.path.exists(key_file_path):
        try:
            with open(key_file_path, 'rb') as key_file:
                key = pickle.load(key_file)
            print(f"Using existing encryption key from {key_file_path}")
            return key
        except Exception as e:
            print(f"Error loading existing key file: {e}")
    
    # If no key exists, generate a new one
    key = generate_key()  # Uses the existing generate_key function
    
    # Save the key to file for future use
    try:
        with open(key_file_path, 'wb') as key_file:
            pickle.dump(key, key_file)
        print(f"Generated new encryption key and saved to {key_file_path}")
    except Exception as e:
        print(f"Warning: Could not save encryption key to file: {e}")
        print(f"Consider saving this key manually: {base64.b64encode(key).decode()}")
    
    return key


def ff1_encrypt(key, number, domain_size):
    """
    Format-preserving encryption using a simplified FF1-based approach.
    This guarantees a permutation (no collisions) for the given domain size.
    """
    # Convert number to bytes for encryption
    number_bytes = str(number).encode()
    
    # Create a deterministic IV based on domain size
    iv = struct.pack('<Q', domain_size) + struct.pack('<Q', 0)
    
    # Create and use cipher in ECB mode for simplicity
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    
    # Pad the number bytes to ensure it's a multiple of 16
    padded = number_bytes + b'\0' * (16 - len(number_bytes) % 16)
    
    # Encrypt the padded bytes
    encrypted_bytes = encryptor.update(padded) + encryptor.finalize()
    
    # Convert to an integer and take modulo to ensure it's within domain
    encrypted_int = int.from_bytes(encrypted_bytes, byteorder='big')
    
    # Ensure the result is within the domain size, maintaining format
    domain_max = 10 ** len(str(number)) - 1
    result = (encrypted_int % domain_max) + 1  # Ensure non-zero
    
    # Handle leading zeros by padding with zeros
    return str(result).zfill(len(str(number)))

def encrypt_single_id(key, id_value):
    """Encrypt a single ID value using the provided key.
    
    Args:
        key: The encryption key
        id_value: The ID to encrypt (string or integer)
        
    Returns:
        Encrypted ID value as a string
    """
    # Handle hyphenated values
    if '-' in str(id_value):
        parts = str(id_value).split('-')
        encrypted_parts = []
        
        for part in parts:
            if part.strip().isdigit():
                num = int(part.strip())
                part_length = len(str(num))
                
                # Get domain size based on input length
                domain_size = 10 ** part_length
                
                encrypted_part = ff1_encrypt(key, num, domain_size)
                encrypted_parts.append(encrypted_part)
            else:
                encrypted_parts.append(part)
                
        return '-'.join(encrypted_parts)
    else:
        # Handle numeric IDs
        try:
            num = int(str(id_value).strip())
            num_length = len(str(num))
            
            # Get domain size based on input length
            domain_size = 10 ** num_length
            
            encrypted_value = ff1_encrypt(key, num, domain_size)
            return encrypted_value
        except ValueError:
            # Return original for non-numeric values
            return str(id_value)

def encrypt_ids(input_file=None, output_file=None, key_output=None):
    # Ensure output folder exists
    output_dir = os.path.dirname(output_file)
    os.makedirs(output_dir, exist_ok=True)
    
    # Check if the key file already exists and load it
    if os.path.exists(key_output):
        try:
            with open(key_output, 'rb') as key_file:
                key = pickle.load(key_file)
            print(f"Using existing encryption key from {key_output}")
        except Exception as e:
            print(f"Error loading existing key: {e}")
            key = generate_key()
            # Save the new key
            with open(key_output, 'wb') as key_file:
                pickle.dump(key, key_file)
            print(f"Generated new encryption key and saved to {key_output}")
    else:
        # Generate a single key for all columns
        key = generate_key()
        # Save the key to a separate file
        with open(key_output, 'wb') as key_file:
            pickle.dump(key, key_file)
        print(f"Generated new encryption key and saved to {key_output}")

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Read header
        header = next(reader)
        
        # Remove ENDPT_ADDRESS column if it exists
        try:
            endpt_address_index = header.index("ENDPOINT_ADDRESS")
            new_header = [col for i, col in enumerate(header) if i != endpt_address_index]
            writer.writerow(new_header)
        except ValueError:
            # If ENDPT_ADDRESS doesn't exist, keep the original header
            writer.writerow(header)
            endpt_address_index = -1
        
        for row in reader:
            encrypted_row = []
            
            for i, value in enumerate(row):
                # Skip the ENDPT_ADDRESS column
                if i == endpt_address_index:
                    continue
                    
                if i <= 1:  # Process the first two columns
                    try:
                        encrypted_value = encrypt_single_id(key, value)
                        encrypted_row.append(encrypted_value)
                    except ValueError:
                        # Handle non-integer values
                        encrypted_row.append(value)
                else:
                    # Keep any other columns unchanged
                    encrypted_row.append(value)
            
            writer.writerow(encrypted_row)

    print(f"Encryption complete. Output saved to {output_file}")
    
    return key