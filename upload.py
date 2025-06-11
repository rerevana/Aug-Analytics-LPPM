from google.cloud import storage
import os # Tambahkan ini untuk memeriksa variabel lingkungan

# Variabel Global
# Ganti dengan nilai yang sesuai untuk kasus penggunaan Anda
BUCKET_NAME = "upload_data_file"  # Ganti dengan nama bucket Anda
SOURCE_DIRECTORY = "data/"  # Ganti dengan path ke folder lokal Anda
DESTINATION_GCS_PREFIX = "" # Awalan path di GCS, bisa kosong jika ingin di root bucket

def upload_folder_contents(bucket_name: str, source_folder: str, destination_prefix: str):
    """Uploads all contents of a local folder to the specified GCS prefix."""

    service_account_json_path = r"D:\Riset\keys.json"  # Menggunakan raw string untuk path Windows
    storage_client = storage.Client.from_service_account_json(service_account_json_path)
    bucket = storage_client.bucket(bucket_name)

    if not os.path.isdir(source_folder):
        print(f"ERROR: Folder sumber '{source_folder}' tidak ditemukan atau bukan direktori.")
        return

    success_count = 0
    failure_count = 0

    for root, _, files in os.walk(source_folder):
        for filename in files:
            local_path = os.path.join(root, filename)
            
            # Membuat path relatif dari folder sumber untuk GCS
            relative_path = os.path.relpath(local_path, source_folder)
            # Mengganti pemisah path Windows dengan pemisah path GCS (/)
            gcs_path = os.path.join(destination_prefix, relative_path).replace("\\", "/")
            
            try:
                blob = bucket.blob(gcs_path)
                print(f"Mengunggah {local_path} ke gs://{bucket_name}/{gcs_path}...")
                blob.upload_from_filename(local_path)
                print(f"  SUKSES: Berkas {local_path} telah berhasil diunggah.")
                success_count += 1
            except Exception as e:
                print(f"  ERROR: Gagal mengunggah {local_path}: {e}")
                failure_count += 1
    
    print(f"\nProses unggah selesai. Berhasil: {success_count}, Gagal: {failure_count}")

if __name__ == '__main__':
    print("Memulai proses unggah berkas...")
    upload_folder_contents(BUCKET_NAME, SOURCE_DIRECTORY, DESTINATION_GCS_PREFIX)
