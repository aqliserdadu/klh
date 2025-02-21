# Penginstalan Docker di Raspberrypi bookworm dan Pemasangan Aplikasi

# Penambahan Repository
	sudo apt-get update
 	sudo apt-get install ca-certificates curl
  	sudo install -m 0755 -d /etc/apt/keyrings
  	sudo curl -fsSL https://download.docker.com/linux/raspbian/gpg -o /etc/apt/keyrings/docker.asc
  	sudo chmod a+r /etc/apt/keyrings/docker.asc

  	echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/raspbian \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  	sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  	sudo apt-get update

#  Penginstalan Docker
	sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin


# Download Image
	sudo docker pull aqliserdadu/klh:2.0


# Penggunaan Container
Penggunaan bridge 

	docker run -d --restart=always --name klh -p 3306:3306 -p 80:80 -v /home/user/FTP:/home/FTP -v /home/user/klh:/home/klh aqliserdadu/klh:2.0
	
Penggunaan host

	docker run -d --restart=always --network host --name klh -v /home/user/FTP:/home/FTP -v /home/user/klh:/home/klh aqliserdadu/klh:2.0

note : pilih salah satu & user sesuaikan dengan nama folder masing-masing

# Extrak Script di home
	git clone https://github.com/aqliserdadu/klh.git

# Cara kerja script
Docker image terdapat interval
1. interval setiap 1 menit akan membaca pembacan file CSV yang ada di folder FTP di home, menjalankan script baca.py
2. interval setiap 1 jam akan melakukan pengiriman data ke server API, menjalankan script sendApi.py untuk penyesuaian alamat API ubah di script sendApi.py

# Crontab
Untuk melakukan pengaturan crontab lakukan di dalam file config

	* * * * * baca.py
akan menjalankan script baca dalam 1 menit, untuk menonaktifkan cukup tambahkan tanda #

	#* * * * * baca.py

Berkaitan dengan Image Docker bisa lihat di 

https://hub.docker.com/r/aqliserdadu/klh
	
