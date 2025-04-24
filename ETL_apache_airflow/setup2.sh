#!/usr/bin/bash

# Update system and install dependencies
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y openjdk-11-jdk python3 python3-pip wget curl tar

# Set Java environment variables
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64' >> ~/.bashrc
source ~/.bashrc

echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Download and install Spark
wget https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
tar -xvzf spark-3.5.4-bin-hadoop3.tgz && \
sudo mv spark-3.5.4-bin-hadoop3 /opt/spark

# Set Spark environment variables
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
source ~/.bashrc

echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# Install PySpark
pip3 install pyspark

# Set PySpark Python environment variable
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
source ~/.bashrc

# Install Jupyter Notebook
pip3 install notebook


# set path after installing jupyter Notebook
echo 'export PATH=$PATH:/home/ubuntu/.local/bin' >> ~/.bashrc

# Reload bashrc to apply changes
source ~/.bashrc

echo "Setup completed! You can now run Jupyter with the command 'jupyter notebook'."





# Download and install Hadoop
# wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz && \
# tar -xvzf hadoop-3.4.1.tar.gz && \
# sudo mv hadoop-3.4.1 /opt/hadoop

# Set Hadoop environment variables
# echo 'export HADOOP_HOME=/opt/hadoop' >> ~/.bashrc
# source ~/.bashrc

# echo 'export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH' >> ~/.bashrc
# source ~/.bashrc

# Verify Hadoop installation
# hadoop version





# --------------------------
# Install Docker and Docker Compose
# --------------------------
# Add Docker's official GPG key:
sudo apt-get update  && \
sudo apt-get install ca-certificates curl && \
sudo install -m 0755 -d /etc/apt/keyrings && \
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
sudo chmod a+r /etc/apt/keyrings/docker.asc
source ~/.bashrc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
source ~/.bashrc

sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin  && \
docker --version






# --------------------------
# Install PostgreSQL
# --------------------------
# sudo apt-get install -y postgresql postgresql-contrib

# Start PostgreSQL service
# sudo systemctl start postgresql
# sudo systemctl enable postgresql

# source ~/.bashrc

# Set up PostgreSQL user and database
# Replace 'myuser' and 'mypassword' with your preferred username and password
# sudo -u postgres psql <<EOF
# CREATE USER myuser WITH PASSWORD 'mypassword';
# CREATE DATABASE mydatabase OWNER myuser;
# ALTER USER myuser WITH SUPERUSER;
# EOF
# source ~/.bashrc
# echo "PostgreSQL installation and setup completed."

# Optional: Add PostgreSQL binaries to PATH
# echo 'export PATH=$PATH:/usr/lib/postgresql/14/bin' >> ~/.bashrc
# source ~/.bashrc

# echo "All tools installed and configured. You can now run 'jupyter notebook' to get started!"





# # --------------------------
# # Install pgAdmin 4 (Web version)
# # --------------------------
# curl https://www.pgadmin.org/static/packages_pgadmin_org.pub | sudo gpg --dearmor -o /usr/share/keyrings/pgadmin.gpg

# sudo sh -c 'echo "deb [signed-by=/usr/share/keyrings/pgadmin.gpg] https://ftp.postgresql.org/pub/pgadmin/pgadmin4/apt/$(lsb_release -cs) pgadmin4 main" > /etc/apt/sources.list.d/pgadmin4.list && apt update'

# sudo apt install -y pgadmin4-web
# source ~/.bashrc

# # Configure pgAdmin
# sudo /usr/pgadmin4/bin/setup-web.sh

# echo "pgAdmin 4 has been installed and configured. Access it via http://localhost/pgadmin4"
# source ~/.bashrc
# # --------------------------
# # Setup Complete
# # --------------------------
# echo "All components installed: Java, Spark, Hadoop, Jupyter, PostgreSQL, and pgAdmin."
