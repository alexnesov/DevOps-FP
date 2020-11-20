wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xzf ta-lib-0.4.0-src.tar.gz
cd ta-lib/

sudo ./configure
sudo make
sudo make install

pip3 install ta-lib

export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
# or for a permanent solution, you’ll have to add /usr/local/lib to /etc/ld.so.conf as root then run /sbin/ldconfig (also as root).
# https://sachsenhofer.io/install-ta-lib-ubuntu-server/

