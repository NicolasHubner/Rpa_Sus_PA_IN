import os
import zipfile

# Define the main directory containing the subfolders
main_folder = './data/bahia'
extract_folder = './data/bahia/PA_ACIMA_2008'

# Create the destination folder if it doesn't exist
os.makedirs(extract_folder, exist_ok=True)

def extract_specific_files(folder, file_extension):
    # Walk through the directory structure
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.zip'):
                # Full path of the zip file
                zip_file_path = os.path.join(root, file)
                
                # Open the zip file
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    # Iterate through the files in the zip archive
                    for member in zip_ref.namelist():
                        # Check if the file has the desired extension (e.g., .dbf)
                        if member.endswith(file_extension):
                            # Extract the file to the destination folder, not maintaining original folder structure
                            zip_ref.extract(member, extract_folder)
                            # Move the file to the root of the extract folder
                            extracted_path = os.path.join(extract_folder, member)
                            final_path = os.path.join(extract_folder, os.path.basename(member))
                            os.rename(extracted_path, final_path)
                            print(f'Extracted {member} to {final_path}')
                            
# Start the extraction process for .dbf files
extract_specific_files(main_folder, '.dbf')

print('All .dbf files have been extracted.')
