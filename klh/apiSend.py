import time
import mysql.connector
from datetime import datetime
import pytz
import requests
import json
import jwt

# Koneksi database MySQL
MYSQL_CONFIG = {
    'host': 'localhost',         # ganti dengan host MySQL Anda
    'user': 'project',               # ganti dengan username MySQL Anda
    'password': '**project**',       # ganti dengan password MySQL Anda
    'database': 'loger'           # ganti dengan nama database MySQL Anda
}

# Konfigurasi endpoint API, folder data, dan path database
API_ENDPOINT = "https://sparing.kemenlh.go.id/api/send-hourly"  # Ganti dengan URL API Anda
API_JWT = "https://sparing.kemenlh.go.id/api/secret-sensor"

tz = pytz.timezone('Asia/Jakarta')

def write_log(message):
    folder="/home/pi/klh/LOG/apiLog.txt"
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    with open(folder, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")

def get_jwt_token():
    try:
        # Kirim permintaan GET ke API untuk mendapatkan token
        response = requests.get(API_JWT)

        # Cek jika response sukses (status code 200)
        if response.status_code == 200:
            # Misalnya token ada di dalam JSON response seperti: {"token": "your_token_value"}
            jwt_token = response.text.strip()
        

            if jwt_token:
                print(f"Token JWT berhasil didapatkan: {jwt_token}")
                return jwt_token
            else:
                print("Token tidak ditemukan dalam response.")
                write_log("Token tidak ditemukan dalam response.")
        else:
            print(f"Gagal mendapatkan token, status code: {response.status_code}")
            write_log("Gagal mendapatkan token, status code:" + response.status_code)

    except requests.exceptions.RequestException as e:
        print(f"Terjadi error saat menghubungi API: {e}")
        write_log("Terjadi error saat menghubungi API:" + e)

# Panggil fungsi untuk mendapatkan token JWT


def send_data_to_api():
    now = datetime.now(tz)
    print(now)

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Eksekusi query untuk mengambil data dari tabel
        cursor.execute("SELECT datetime, ph, tss, cod, debit, nh3n FROM tmp  WHERE date < %s", [now])
        rows = cursor.fetchall()
        conn.commit()
        conn.close()

        # Ambil nama kolom dari hasil query
        columns = [desc[0] for desc in cursor.description]

        # Konversi hasil query ke dalam format JSON
        json_data = [dict(zip(columns, row)) for row in rows]

        # Struktur JSON akhir dengan uid di luar data
        payload = {
            #63d3fe3fa2366e1acce66360
            "uid": "63d3fe3fa2366e1acce66360",  # Ganti 'kode' dengan nilai UID yang sesuai
            "data": json_data
        }

        jwtHeader = {
            "alg" : "HS256",
            "typ" : "JWT"
        }

        # Encode payload menjadi JWT
        keyToken = get_jwt_token()  # Ganti dengan kunci rahasia yang tepat
        # JWT encoding membutuhkan payload dalam bentuk dictionary, bukan string JSON
        enc = jwt.encode(payload, keyToken, algorithm='HS256', headers=jwtHeader)

        print("Payload JWT:", json.dumps(payload, default=str, indent=4))
        print("Encoded JWT:", enc)

        # Kirim data ke API
        # Biasanya, Anda akan menggunakan requests untuk mengirim data
        headers = {'Authorization': f'Bearer {keyToken}', 'Content-Type': 'application/json'}
        response = requests.post(API_ENDPOINT, json={"token": enc}, headers=headers)
        
        if response.status_code == 200:
            print("Data berhasil dikirim ke API")
            print(response.text)
            write_log("Data berhasil dikirim ke API")
            write_log(f"Pesan Berhasil: {response.text}")
            #jika berhasil hapus data didatabase
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()

            # Eksekusi query untuk mengambil data dari tabel
            cursor.execute("DELETE FROM tmp WHERE date < %s", [now])
            
            conn.commit()
            conn.close()


        else:
           print(f"Gagal mengirim data, status code: {response.status_code}")
           print(f"Pesan error: {response.text}")
           write_log(f"Gagal mengirim data, status code: {response.status_code}")
           write_log(f"Pesan error: {response.text}")

    except Exception as e:
        print(f"[{datetime.now()}] Error pada koneksi database: {e}")
        write_log(f"Error pada koneksi database : {e}")

# Panggil fungsi untuk mengirim data
send_data_to_api()

