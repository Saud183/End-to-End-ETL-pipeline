from hdfs import InsecureClient


# HDFS configuration
hdfs_url = 'http://localhost:9000'  # Replace with your HDFS host and port
hdfs_user = '<username>'                    # Replace with your HDFS user

# Initialize the client
client = InsecureClient(hdfs_url, user=hdfs_user)

# Local file path and HDFS destination path
local_file_path = '<path_to_local_file>'   # e.g., '/path/to/file.txt'
hdfs_dest_path = '<path_in_hdfs>'         # e.g., '/user/username/file.txt'

try:
    # Upload the file to HDFS
    client.upload(hdfs_dest_path, local_file_path, overwrite=True)
    print(f"File uploaded to {hdfs_dest_path}")
except Exception as e:
    print(f"Failed to upload file: {e}")
