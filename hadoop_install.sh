#!/bin/bash

REL_DIR="$(dirname $0)"
TMP="/tmp/.hadoopinstall$$_"

# Dirty, little hackish script to get Hadoop installed and running...
# Author: Jon Miller http://jonebird.com/ - jonEbird@gmail.com
# 
# See http://hadoop.apache.org/core/docs/r0.18.3/quickstart.html
# 
HADOOP_VERSION=${1:-0.18.3}
HADOOP_INSTALL_DIR=${2:-${REL_DIR}}

MEDIA="http://mirror.cloudera.com/apache/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"

#----------------------------------------------------------------------
# sanity checks...

# check #1: basic usage
if [ ! -d "${HADOOP_INSTALL_DIR}" ]; then
    echo "Error: Specified install directory \"${HADOOP_INSTALL_DIR}\" does not exist. Please try again."
    exit 1
fi

# check #2: Java version
JAVA_VERSION=$(java -version 2>&1 | sed -n '/^java version/s/^java version "\([0-9]*\.[0-9]*\)\..*$/\1/p')
if [ $(echo "$JAVA_VERSION >= 1.5" | bc -l) -eq 1 ]; then
    echo "Good: Your version of Java ${JAVA_VERSION} is new enough. JAVA_VERSION >= 1.5"
else
    echo "Bad: Your version of Java \"${JAVA_VERSION}\" is not new enough. JAVA_VERSION >= 1.5"
    echo "     Either upgrade or update your PATH so I find the proper java you want me to. Exiting."
    exit 2
fi

# check #3: Can you ssh to yourself?
# before going any further, if there is no .ssh/ dir setup, let's help out
if [ ! -d ~/.ssh ]; then
    mkdir -m 0700 ~/.ssh
    ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
    cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
fi

ssh -o StrictHostKeyChecking=no localhost "uname -n" > ${TMP}hostname 2>/dev/null &
sshpid=$!
sleep 3 # 3 seconds is plenty in my book...
if ps -fp $sshpid >/dev/null 2>&1; then
    kill $sshpid
    echo "Bad: Looks like my ssh attempt to localhost is still running after 3s. Killing that and exiting."
    echo "     Need to resolve that issue to where you can ssh to localhost without a password first."
    exit 3
else
    if [ "$(cat ${TMP}hostname)" == "$(uname -n)" ]; then
	echo "Good: Hurrah, we can ssh to localhost without needing a password. I'm so happy..."
    else
	echo "Bad: It appears that I am unable to ssh passwordless to localhost. Please fix that then rerun me."
	echo "     Could be that you just do not have sshd running?"
	exit 4
    fi
fi

#----------------------------------------------------------------------
# Okay, good to go I think... 

readlink_r() {
    local symlink="$1"
    local nlink="$(readlink $symlink)"
    if [ -z "${nlink}" ]; then
	echo $symlink
	return 0
    else
	readlink_r $nlink
    fi
}
JAVA_HOME=$(readlink_r $(which java) | sed 's|/bin/java$||g')

echo "Pulling down media and extracting to ${HADOOP_INSTALL_DIR}/hadoop-${HADOOP_VERSION}/. Probably a good idea to be patient."
curl -s $MEDIA | tar -C ${HADOOP_INSTALL_DIR} -xzf -

HADOOP_DIR="${HADOOP_INSTALL_DIR}/hadoop-${HADOOP_VERSION}"

echo "Setting your JAVA_HOME in the ${HADOOP_DIR}/conf/hadoop-env.sh"
sed -i "/^# export JAVA_HOME/aexport JAVA_HOME=${JAVA_HOME}" ${HADOOP_DIR}/conf/hadoop-env.sh

echo "Populating your ${HADOOP_DIR}/conf/hadoop-site.xml config with suggested defaults."
cat <<EOF > ${HADOOP_DIR}/conf/hadoop-site.xml
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>localhost:9000</value>
  </property>
  <property>
    <name>mapred.job.tracker</name>
    <value>localhost:9001</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
EOF

echo "Formating a new HDFS filesystem"
${HADOOP_DIR}/bin/hadoop namenode -format

cat <<EOF
If you're lucky, we're all set to go.

To start all of the Hadoop daemons:
   ${HADOOP_DIR}/bin/start-all.sh

To stop the daemons, then run:
   ${HADOOP_DIR}/bin/stop-all.sh  

Finally, you're probably one out of a handful of people to have ran this, so you might need to view:
http://hadoop.apache.org/core/docs/r${HADOOP_VERSION}/quickstart.html

EOF

# cleanup
[ -n "${TMP}" ] && rm -rf ${TMP}*
exit 0
