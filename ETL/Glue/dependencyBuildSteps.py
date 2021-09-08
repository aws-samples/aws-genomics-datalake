/sbin/sysctl -w net.ipv4.tcp_keepalive_time=200 net.ipv4.tcp_keepalive_intvl=200 net.ipv4.tcp_keepalive_probes=5
sudo yum install -y g++ cmake git
sudo yum install -y lz4
sudo yum install -y lz4-devel
git clone https://github.com/hail-is/hail.git
cd hail/hail && git fetch && git checkout
sudo yum groupinstall 'Development Tools' —y
sudo yum install java-1.8.0
sudo alternatives --config java
sudo yum search java | grep openjdk
sudo yum install java-1.8.0-openjdk-headless.x86_64
sudo yum install java-1.8.0-openjdk-devel.x86_64
sudo update-alternatives --config java
sudo update-alternatives --config javac
make install HAIL_COMPILE_NATIVES=1 SPARK_VERSION=2.4.4 SCALA_VERSION=2.11.12
cd build
pip download decorator==4.2.1
cd /home/ec2-user/hail/hail/python
pip install --upgrade --target=/home/ec2-user/hail/hail/python/ nest_asyncio
pip install --target=/home/ec2-user/hail/hail/python/ "humanize==1.0.0"
pip install --upgrade --target=/home/ec2-user/hail/hail/python/ aiohttp
pip install --upgrade --target=/home/ec2-user/hail/hail/python/ google
pip install --upgrade google-auth google-auth-httplib2 google-api-python-client
pip install --upgrade --target=/home/ec2-user/hail/hail/python/ google.auth
pip install --target=/home/ec2-user/hail/hail/python/ google.api.core
zip hail-python.zip -r .
aws s3 cp hail-python.zip s3://coviddatasalaunch/artifacts_new_2/
--additional-python-modules decorator,nest_asyncio,humanize,nest_asyncio,google-api-core,parsimonious,deprecated,boke
