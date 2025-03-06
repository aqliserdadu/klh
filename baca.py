import os
import csv
import time
import mysql.connector
from datetime import datetime
import pytz
import math
from dotenv import load_dotenv

# Load environment variables
env_path = "/home/klh/config/.env"  # .env file path
if not load_dotenv(dotenv_path=env_path):
    print(f"Error: .env file not found at {env_path}")
    exit(1)
    
# Make sure this is 1 or 0, as environment variables are strings by default
BACACSV = int(os.getenv('BACACSV')) 
HOST = os.getenv('HOST')
USER = os.getenv('USERS')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DATABASE')
TIMEZONA = os.getenv('TIMEZONA')

# MySQL connection configuration
MYSQL_CONFIG = {
    'host': HOST,
    'user': USER,
    'password': PASSWORD,
    'database': DATABASE
}

# Timezone configuration
tz = pytz.timezone(TIMEZONA)

def write_log(message):
    log_file_path = "/home/klh/LOG/csvLog.txt"
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")

def prosesCsv():
    folder = "/home/FTP"
    try:
        files = os.listdir(folder)
    except Exception as e:
        print("Gagal membuka folder FTP:", e)
        write_log("Gagal membuka folder FTP: " + str(e))
        return

    # Filter file CSV saja
    csv_files = [f for f in files if f.lower().endswith('.csv')]

    if not csv_files:
        print("Folder kosong, tidak ada file CSV yang ditemukan.")
        write_log("Folder kosong, tidak ada file CSV yang ditemukan.")
        return

    # Membuka koneksi database sekali saja untuk semua file
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        # Buat tabel jika belum ada
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATETIME,
                datetime BIGINT DEFAULT 0,
                pH FLOAT DEFAULT 0,
                nh3n FLOAT DEFAULT 0,
                tss FLOAT DEFAULT 0,
                debit FLOAT DEFAULT 0,
                cod FLOAT DEFAULT 0,
                status TEXT,
                keterangan TEXT,
                dateterkirim DATETIME
            )
        ''')
        conn.commit()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tmp (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATETIME,
                datetime BIGINT DEFAULT 0,
                pH FLOAT DEFAULT 0,
                nh3n FLOAT DEFAULT 0,
                tss FLOAT DEFAULT 0,
                debit FLOAT DEFAULT 0,
                cod FLOAT DEFAULT 0,
                status TEXT,
                keterangan TEXT,
                dateterkirim DATETIME
            )
        ''')
        conn.commit()
        
    except Exception as e:
        print(f"[{datetime.now()}] Error pada koneksi database: {e}")
        write_log("Error pada koneksi database: " + str(e))
        return

    for filename in csv_files:
        filepath = os.path.join(folder, filename)
        print(f"Memproses file: {filename}")
        try:
            with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=';')
                # Melewati baris pertama (misal informasi atau header tambahan)
                try:
                    next(reader)
                except StopIteration:
                    print(f"File {filename} kosong atau tidak sesuai format.")
                    write_log(f"File {filename} kosong atau tidak sesuai format.")
                    continue

                # Membaca header asli
                original_headers = next(reader)
                print("Header asli:", original_headers)

                # Mapping kolom berdasarkan kata kunci
                new_headers = []
                for col in original_headers:
                    lower_col = col.lower()
                    if "interval" in lower_col:
                        new_headers.append("Interval_Timestamp")
                    elif "ph" in lower_col:
                        new_headers.append("pH")
                    elif "tsseq" in lower_col:
                        new_headers.append("TSSeq")
                    elif "codeq" in lower_col:
                        new_headers.append("CODeq")
                    elif "debit" in lower_col:
                        new_headers.append("Debit")
                    elif "nh3-n" in lower_col:
                        new_headers.append("NH3-N")
                    else:
                        new_headers.append(col.strip())
                print("Header yang telah diganti:", new_headers)

                # Menampilkan beberapa baris data untuk pengecekan
                # Mengambil 5 baris pertama
                sample_data = []
                data_rows = []
                for i, row in enumerate(reader):
                    if i < 5:
                        sample_data.append(row)
                    data_rows.append(row)
                print("Sample data:")
                for r in sample_data:
                    print(r)
        except Exception as e:
            print(f"Terjadi kesalahan saat membaca file CSV {filename}: {e}")
            write_log("Terjadi kesalahan saat membaca file CSV " + filename + ": " + str(e))
            continue

        # Memasukkan data ke database
        for index, row in enumerate(data_rows):
            try:
                # Buat dictionary baris dengan header yang telah dimapping
                row_data = dict(zip(new_headers, row))
                
                # Parsing nilai untuk masing-masing kolom
                # Default jika tidak ada atau parsing gagal
                Interval_Timestamp = None
                unix_dt = 0
                if "Interval_Timestamp" in row_data:
                    try:
                        # Misal format: 'YYYY-MM-DD HH:MM:SS'
                        Interval_Timestamp = datetime.strptime(row_data["Interval_Timestamp"], '%Y-%m-%d %H:%M:%S')
                        unix_dt = int(time.mktime(Interval_Timestamp.timetuple()))
                    except Exception as ex:
                        write_log(f"Baris {index} file {filename}: Gagal parsing Interval_Timestamp: {ex}")
                        Interval_Timestamp = None
                        unix_dt = 0

                # Parsing nilai numerik dengan fallback ke 0 bila gagal
                def to_float(value):
                    try:
                        return float(value)
                    except:
                        return 0

                ph = to_float(row_data.get("pH", 0))
                tss = to_float(row_data.get("TSSeq", 0))
                cod = to_float(row_data.get("CODeq", 0))
                debit = to_float(row_data.get("Debit", 0))
                nh3n = to_float(row_data.get("NH3-N", 0))


                def replace_nan(value):
                    return 0 if isinstance(value, float) and math.isnan(value) else value
                
                 # Ganti nilai NaN atau null dengan 0 sebelum dimasukkan ke database
                ph = replace_nan(ph)
                tss = replace_nan(tss)
                cod = replace_nan(cod)
                debit = replace_nan(debit)
                nh3n = replace_nan(nh3n)
                
                #cek jika ada waktu di jam ganjil tidak dimasukan database, melainkan pertanda sedang kalibrasi
                if Interval_Timestamp.minute % 2 == 0 :

                    cursor.execute(
                            "INSERT INTO tmp (date, datetime, pH, tss, cod, debit, nh3n) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (Interval_Timestamp, unix_dt, ph, tss, cod, debit, nh3n)
                    )
                else:
                    print("Sedang melakukan kalibrasi, Nilai data")
                    write_log("Sedang melakukan kalibrasi, Nilai data")
                    

                
            except Exception as e:
                print(f"[{datetime.now()}] Error memasukkan baris {index} pada file {filename}: {e}")
                write_log("Error memasukkan baris " + str(index) + " pada file " + filename + " : " + str(e))
        try:
            conn.commit()
        except Exception as e:
            write_log("Gagal commit data untuk file " + filename + ": " + str(e))
            print(f"Gagal commit data untuk file {filename}: {e}")

        # Hapus file CSV yang sudah diproses
        try:
            os.remove(filepath)
            print(f"[{datetime.now()}] File {filename} telah dihapus.\n")
            write_log("File " + filename + " telah dihapus")
        except Exception as e:
            print(f"[{datetime.now()}] Error menghapus file {filename}: {e}")
            write_log("Error menghapus file " + filename + " : " + str(e))

    cursor.close()
    conn.close()
    print(f"[{datetime.now()}] Semua data telah diproses dan dimasukkan ke database MySQL.")
    write_log("Semua data telah diproses dan dimasukkan ke database MySQL.")

if __name__ == "__main__":
    
    if BACACSV ==1:  
        prosesCsv()
    else:
        print("Pembacaan CSV Sedang Tidak Aktif")
        write_log("Pembacaan CSV Sedang Tidak Aktif")