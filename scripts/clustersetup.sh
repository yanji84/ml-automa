curl -o /tmp/install_salt.sh -L https://bootstrap.saltstack.com && sh /tmp/install_salt.sh -Z -M git v2015.5.3

# reuse ssh key pair found in the repo
export PUBLIC_KEY=`cat ~/.ssh/id_rsa.pub | cut -d ' ' -f 2`
cat > /etc/salt/roster <<EOF
node1:
  host: 158.85.79.185
  priv: /root/.ssh/id_rsa
node2:
  host: 158.85.79.186
  priv: /root/.ssh/id_rsa
node3:
  host: 158.85.79.184
  priv: /root/.ssh/id_rsa
EOF

mv /etc/salt/master /etc/salt/master~orig
cat > /etc/salt/master <<EOF
file_roots:
  base:
    - /srv/salt
fileserver_backend:
  - roots
pillar_roots:
  base:
    - /srv/pillar
EOF

mkdir -p /srv/{salt,pillar} && service salt-master restart

salt-ssh -i '*' cmd.run 'uname -a'

cat > /srv/salt/top.sls <<EOF
base:
 '*':
   - hosts
   - root.ssh
   - root.bash
EOF

cat > /srv/salt/hosts.sls <<EOF
localhost-hosts-entry:
  host.present:
    - ip: 127.0.0.1
    - names:
      - localhost
node1-fqdn-hosts-entry:
  host.present:
    - ip: 158.85.79.185
    - names:
      - node1.projectx.net
node2-fqdn-hosts-entry:
  host.present:
    - ip: 158.85.79.186
    - names:
      - node2.projectx.net
node3-fqdn-hosts-entry:
  host.present:
    - ip: 158.85.79.184
    - names:
      - node3.projectx.net
node1-hosts-entry:
  host.present:
    - ip: 158.85.79.185
    - names:
      - node1
node2-hosts-entry:
  host.present:
    - ip: 158.85.79.186
    - names:
      - node2
node3-hosts-entry:
  host.present:
    - ip: 158.85.79.184
    - names:
      - node3
EOF

mkdir /srv/salt/root
cat > /srv/salt/root/ssh.sls <<EOF
$PUBLIC_KEY:
 ssh_auth.present:
   - user: root
   - enc: ssh-rsa
   - comment: root@node1
EOF

cat > /srv/salt/root/bash_profile <<'EOF'
# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
 . ~/.bashrc
fi

# User specific environment and startup programs
export PATH=$PATH:$HOME/bin
EOF

cat > /srv/salt/root/bashrc <<'EOF'
# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
 . /etc/bashrc
fi

# User specific aliases and functions
alias rm='rm -i'
alias cp='cp -i'
alias mv='mv -i'

# Java
export JAVA_HOME="$(readlink -f $(which java) | grep -oP '.*(?=/bin)')"

# Spark
export SPARK_HOME="/usr/local/spark"
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Hadoop
export HADOOP_HOME="/usr/local/hadoop"
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Spark (part 2, should come after hadoop setup)
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
EOF

cat > /srv/salt/root/bash.sls <<EOF
/root/.bash_profile:
  file.managed:
    - source: salt://root/bash_profile
    - overwrite: true
/root/.bashrc:
  file.managed:
    - source: salt://root/bashrc
    - overwrite: true
EOF

salt-ssh '*' state.highstate

salt-ssh '*' cmd.run 'yum install -y yum-utils'
salt-ssh '*' cmd.run 'yum install -y epel-release'
salt-ssh '*' cmd.run 'yum update -y'
salt-ssh '*' cmd.run 'yum install -y java-1.7.0-openjdk-headless'

mkdir /srv/salt/spark
cat > /srv/salt/spark/slaves <<EOF
node1.projectx.net
node2.projectx.net
node3.projectx.net
EOF

cat > /srv/salt/spark.sls <<EOF
spark:
  archive.extracted:
    - name: /usr/local/
    - source: http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-without-hadoop.tgz
    - source_hash: md5=5b2774df2eb6b9fd4d65835471f7387d
    - archive_format: tar
    - tar_options: -z --transform=s,/*[^/]*,spark,
    - if_missing: /usr/local/spark/
/usr/local/spark/conf/slaves:
  file.managed:
    - source: salt://spark/slaves
    - overwrite: true
EOF

# mount a filesystem onto the secondary disk
salt-ssh '*' cmd.run 'mkdir -m 777 /data'
salt-ssh '*' cmd.run 'mkfs.ext4 /dev/xvdc'
salt-ssh '*' cmd.run 'echo "/dev/xvdc /data ext4 defaults,noatime 0 0" >> /etc/fstab'
salt-ssh '*' cmd.run 'mount /data'

mkdir /srv/salt/hadoop
cat > /srv/salt/hadoop/masters <<EOF
node1.projectx.net
EOF

cat > /srv/salt/hadoop/slaves <<EOF
node1.projectx.net
node2.projectx.net
node3.projectx.net
EOF

cat > /srv/salt/hadoop/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://node1.projectx.net:9000</value>
  </property>
</configuration>
EOF

cat > /srv/salt/hadoop/etc/hadoop/mapred-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

cat > /srv/salt/hadoop/hdfs-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.data.dir</name>
    <value>/data/hdfs</value>
  </property>
</configuration>
EOF

cat > /srv/salt/hadoop/yarn-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>node1.projectx.net:8025</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>node1.projectx.net:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>node1.projectx.net:8050</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
    <description>Whether virtual memory limits will be enforced for containers</description>
  </property>
</configuration>
EOF

cat > /srv/salt/hadoop.sls <<EOF
hadoop:
  archive.extracted:
    - name: /usr/local/
    - source: http://apache.claz.org/hadoop/core/hadoop-2.7.1/hadoop-2.7.1.tar.gz
    - source_hash: md5=203e5b4daf1c5658c3386a32c4be5531
    - archive_format: tar
    - tar_options: -z --transform=s,/*[^/]*,hadoop,
    - if_missing: /usr/local/hadoop/
/usr/local/hadoop/etc/hadoop/masters:
  file.managed:
    - source: salt://hadoop/masters
    - overwrite: true 
/usr/local/hadoop/etc/hadoop/slaves:
  file.managed:
    - source: salt://hadoop/slaves
    - overwrite: true 
/usr/local/hadoop/etc/hadoop/core-site.xml:
  file.managed:
    - source: salt://hadoop/core-site.xml
    - overwrite: true 
/usr/local/hadoop/etc/hadoop/mapred-site.xml:
  file.managed:
    - source: salt://hadoop/mapred-site.xml
    - overwrite: true 
/usr/local/hadoop/etc/hadoop/hdfs-site.xml:
  file.managed:
    - source: salt://hadoop/hdfs-site.xml
    - overwrite: true 
/usr/local/hadoop/etc/hadoop/yarn-site.xml:
  file.managed:
    - source: salt://hadoop/yarn-site.xml
    - overwrite: true 
/data/hdfs:
  file.directory
EOF

salt-ssh '*' state.apply spark
salt-ssh '*' state.apply hadoop

hadoop namenode -format