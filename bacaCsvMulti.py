import os
import pandas as pd
import time
import mysql.connector
from datetime import datetime
import pytz

# Koneksi database MySQL
MYSQL_CONFIG = {
    'host': 'localhost',         # ganti dengan host MySQL Anda
    'user': 'project',          # ganti dengan username MySQL Anda
    'password': '**project**',      # ganti dengan password MySQL Anda
    'database': 'loger'  # ganti dengan nama database MySQL Anda
}

# Tentukan path folder yang berisi file CSV

tz = pytz.timezone('Asia/Jakarta')
def write_log(message):
    folder="LOG/csvLog.txt"
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    with open(folder, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")


def prosesCsv():
    folder = "/home/pi/FTP"
    files = os.listdir(folder)

    # Filter file CSV saja
    csv_files = [f for f in files if f.lower().endswith('.csv')]

    if not csv_files:
        print("Folder kosong, tidak ada file CSV yang ditemukan.")
        write_log("Folder kosong, tidak ada file CSV yang ditemukan.")
    else:
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
                    ph FLOAT DEFAULT 0,
                    nh3n FLOAT DEFAULT 0,
                    tss FLOAT DEFAULT 0,
                    debit FLOAT DEFAULT 0,
                    cod FLOAT DEFAULT 0
                )
            ''')
            conn.commit()
        except Exception as e:
            print(f"[{datetime.now()}] Error pada koneksi database: {e}")
            write_log("Error pada koneksi database : " + str(e))
            return  # Keluar dari fungsi jika gagal koneksi

        for filename in csv_files:
            filepath = os.path.join(folder, filename)
            print(f"Memproses file: {filename}")
            try:
                # Baca file CSV dengan separator ';'
                data = pd.read_csv(filepath, sep=';', skiprows=1)
            except Exception as e:
                print(f"Terjadi kesalahan saat membaca file CSV {filename}: {e}")
                write_log("Terjadi kesalahan saat membaca file CSV " + filename + " : " + str(e))
                continue  # Lanjutkan ke file berikutnya

            # Tampilkan header asli
            original_headers = data.columns.tolist()
            print("Header asli:", original_headers)

            # Mapping kolom berdasarkan kata kunci
            new_column_names = {}
            for col in original_headers:
                lower_col = col.lower()
                if "interval" in lower_col:
                    new_column_names[col] = "Interval_Timestamp"
                elif "ph" in lower_col:
                    new_column_names[col] = "pH"
                elif "tsseq" in lower_col:
                    new_column_names[col] = "TSSeq"
                elif "codeq" in lower_col:
                    new_column_names[col] = "CODeq"
                elif "debit" in lower_col:
                    new_column_names[col] = "Debit"
                elif "nh3-n" in lower_col:
                    new_column_names[col] = "NH3-N"
                else:
                    new_column_names[col] = col.strip()

            # Ganti nama kolom sesuai mapping
            data.rename(columns=new_column_names, inplace=True)
            print("Header yang telah diganti:", data.columns.tolist())
            
            # Konversi nilai pengukuran ke numeric jika kolomnya ada
            if "pH" in data.columns:
                data['pH'] = pd.to_numeric(data['pH'], errors='coerce')
            if "TSSeq" in data.columns:
                data['TSSeq'] = pd.to_numeric(data['TSSeq'], errors='coerce')
            if "CODeq" in data.columns:
                data['CODeq'] = pd.to_numeric(data['CODeq'], errors='coerce')
            if "Debit" in data.columns:
                data['Debit'] = pd.to_numeric(data['Debit'], errors='coerce')
            if "NH3-N" in data.columns:
                data['NH3-N'] = pd.to_numeric(data['NH3-N'], errors='coerce')
            
            # Konversi kolom Timestamp ke objek datetime dan UNIX timestamp
            if 'Interval_Timestamp' in data.columns:
                data['Interval_Timestamp'] = pd.to_datetime(data['Interval_Timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                data['datetime'] = data['Interval_Timestamp'].apply(
                    lambda dt: int(time.mktime(dt.timetuple())) if pd.notnull(dt) else None
                )
            
            # Menampilkan beberapa baris data untuk pengecekan
            tampungData = data.head()
            print(tampungData)

            # Memasukkan data ke database
            for index, row in data.iterrows():
                try:
                    Interval_Timestamp = row['Interval_Timestamp'] if 'Interval_Timestamp' in row else None
                    datetimes = row['datetime'] if 'datetime' in row else None
                    ph = row.get('pH', 0)
                    tss = row.get('TSSeq', 0)
                    cod = row.get('CODeq', 0)
                    debit = row.get('Debit', 0)
                    nh3n = row.get('NH3-N', 0)

                    cursor.execute(
                        "INSERT INTO data (date, datetime, ph, tss, cod, debit, nh3n) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (Interval_Timestamp, datetimes, ph, tss, cod, debit, nh3n)
                    )
                except Exception as e:
                    print(f"[{datetime.now()}] Error memasukkan baris {index} pada file {filename}: {e}")
                    write_log("Error memasukkan baris " + str(index) + " pada file " + filename + " : " + str(e))
            
            conn.commit()
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

#def main():
#    while True:
#        prosesCsv()
#        time.sleep(5)
prosesCsv()


#if __name__ == "__main__":
#    try:
#        main()
#    except KeyboardInterrupt:
#        print("\n[INFO] Program dihentikan oleh pengguna.")
#    except Exception as e:
#        print(f"\n[ERROR] Terjadi kesalahan fatal: {e}")
