from pyffx import Integer
import os
import csv
import pickle
env = os.path.dirname(os.path.abspath(__file__))

def generate_key():
    return os.urandom(16)  # 128-bit key

def encrypt(key, number, length):
    # Create an FFX encryptor with specified length
    ffx = Integer(key, length=length)
    return ffx.encrypt(number)

def encrypt_single_id(key, id_value, existing_encrypted_values=None):
    """Encrypt a single ID value using the provided key.
    
    Args:
        key: The encryption key
        id_value: The ID to encrypt (string or integer)
        existing_encrypted_values: Set of already encrypted values to avoid duplicates
        
    Returns:
        Encrypted ID value as a string
    """
    if existing_encrypted_values is None:
        existing_encrypted_values = set()
        
    # Handle hyphenated values
    if '-' in str(id_value):
        parts = str(id_value).split('-')
        encrypted_parts = []
        
        for part in parts:
            if part.strip().isdigit():
                num = int(part.strip())
                part_length = len(str(num))
                
                # Ensure no duplicates by trying different lengths if needed
                encrypted_part = encrypt(key, num, part_length)
                attempts = 0
                max_attempts = 10
                
                while str(encrypted_part) in existing_encrypted_values and attempts < max_attempts:
                    # Try with a slightly different length to avoid collision
                    adjusted_length = part_length + attempts + 1
                    encrypted_part = encrypt(key, num, adjusted_length)
                    attempts += 1
                
                existing_encrypted_values.add(str(encrypted_part))
                encrypted_parts.append(str(encrypted_part))
            else:
                encrypted_parts.append(part)
                
        return '-'.join(encrypted_parts)
    else:
        # Handle numeric IDs
        try:
            num = int(str(id_value).strip())
            num_length = len(str(num))
            
            # Ensure no duplicates
            encrypted_value = encrypt(key, num, num_length)
            attempts = 0
            max_attempts = 10
            
            while str(encrypted_value) in existing_encrypted_values and attempts < max_attempts:
                # Try with a slightly different length to avoid collision
                adjusted_length = num_length + attempts + 1
                encrypted_value = encrypt(key, num, adjusted_length)
                attempts += 1
            
            existing_encrypted_values.add(str(encrypted_value))
            return str(encrypted_value)
        except ValueError:
            # Return original for non-numeric values
            return str(id_value)

def encrypt_ids(input_file=None, output_file=None, key_output=None):
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

    # Track already encrypted values to prevent duplicates
    encrypted_values = set()

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write header
        header = next(reader)
        writer.writerow(header)
        
        for row in reader:
            encrypted_row = []
            
            for i, value in enumerate(row):
                if i <= 1:  # Process the first two columns
                    try:
                        encrypted_value = encrypt_single_id(key, value, encrypted_values)
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

# Example usage:
if __name__ == "__main__":
    
    # Set default file paths if not provided
    input_file = f'{env}/dicom_urls.csv'
    output_file = f'{env}/encrypted_output.csv'
    key_output = f'{env}/encryption_key.pkl'
        
    key = encrypt_ids(input_file, output_file, key_output)
    
    # Example of how to use the single ID encryption function
    # test_id = "12345-6"
    # encrypted_id = encrypt_single_id(key, test_id)
    # print(f"Original ID: {test_id}, Encrypted ID: {encrypted_id}")