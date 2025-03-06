Penggunaan Container
Penggunaan bridge

  docker run -d --restart=always --name klh -p 3306:3306 -p 80:80 -v /home/user/FTP:/home/FTP -v /home/user/klh:/home/klh aqliserdadu/klh:2.0

Penggunaan host

  docker run -d --restart=always --network host --name klh -v /home/user/FTP:/home/FTP -v /home/user/klh:/home/klh aqliserdadu/klh:2.0

note : pilih salah satu & user sesuaikan dengan nama folder masing-masing
Extrak Script di home

  git clone https://github.com/aqliserdadu/klh.git

Cara kerja script

Docker image terdapat interval

1.interval setiap 1 menit akan membaca pembacan file CSV yang ada di folder FTP di home, menjalankan script baca.py
2.interval setiap 1 jam akan melakukan pengiriman data ke server API, menjalankan script sendApi.py untuk penyesuaian alamat API ubah di script sendApi.py
3.interval setiap di menit 4,8,12 akan melakukan pengiriman data ke server API, menjalankan script retrySendApi.py untuk penyesuaian alamat API ubah di script retrySendApi.py


Environment

rename file env di folder config/env menjadi .env

Crontab

Untuk melakukan pengaturan crontab lakukan di dalam file config/crontab

* * * * * baca.py
0 * * * * sendApi.py
4,8,12 * * * * retrySendApi.py

akan menjalankan script baca dalam 1 menit, untuk menonaktifkan cukup tambahkan tanda #

#* * * * * baca.py

Berkaitan dengan Image Docker bisa lihat di

  https://hub.docker.com/r/aqliserdadu/klh
