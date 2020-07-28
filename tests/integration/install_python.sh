#!/bin/bash

set -e

if [ -d "/usr/src/Python-3.6.10" ] 
then
	echo "Python already installed in /usr/src/Python-3.6.10"
	exit
fi

# install python interpreter globally
yum install -y wget gcc openssl-devel bzip2-devel libffi-devel
pushd /usr/src
wget https://www.python.org/ftp/python/3.6.10/Python-3.6.10.tgz
tar xzf Python-3.6.10.tgz
pushd Python-3.6.10
  ./configure --enable-optimizations
  make altinstall
rm /usr/src/Python-3.6.10.tgz
