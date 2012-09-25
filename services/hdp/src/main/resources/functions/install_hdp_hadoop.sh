#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -x

# this requires that install_hdp_hadoop() has already set up
# the relevant env variables
#

# see http://bit.ly/QcFWqv 
# http://docs.hortonworks.com/CURRENT/index.htm#Deploying_Hortonworks_Data_Platform/Using_gsInstaller/System_Requirements_For_Test_And_Production_Clusters.htm
# the layout is OS-specific; this script only supports Centos6, unless OS_VERSION is changed,
#
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/centos5/hdp.repo
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/centos6/hdp.repo
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/suse11/hdp.repo

function register_hortonworks_repo() {

  rm -f /etc/yum.repos.d/hdp-whirr-*.repo
  local REPOFILE=/etc/yum.repos.d/hdp-whirr-${REPO}-${HDP_VERSION}.repo
  local baseurl="http://${REPO_HOST}/HDP-${HDP_VERSION}/repos/${OS_VERSION}"
  local utilsurl="http://${REPO_HOST}/HDP-UTILS-${HDP_VERSION}/repos/${OS_VERSION}"
  local keyurl= "${baseurl}/RPM-GPG-KEY-Jenkins"
  
  cat > $REPOFILE << EOF
[HDP-${REPO}]
name=Hortonworks Data Platform Version - HDP-${HDP_VERSION}
baseurl=${baseurl}
gpgcheck=0
enabled=1
priority=1
gpgcheck=1
gpgkey=${keyurl}

[HDP-UTILS-${REPO}]
name=Hortonworks Data Platform Utils Version - HDP-UTILS-${REPO}
baseurl=${utilsurl}
gpgcheck=1
gpgkey=${keyurl}
enabled=1
priority=1
EOF

  echo "installed new repo file ${REPOFILE} with repository ${baseurl} and ${utilsurl}"
  echo "About to update yum"
  retry_yum update -y 
}


function install_hdp_hadoop() {
  local OPTIND
  local OPTARG
  local retval
  if [ "$INSTALL_HADOOP_DONE" == "1" ]; then
    echo "Hadoop is already installed."
    return;
  fi

  HDP_VERSION=1.1.0.15
  REPO=${REPO:-hdp1}
  REPO_HOST=${REPO_HOST:-public-repo-1.hortonworks.com}
  HADOOP_VERSION=${HADOOP_VERSION:-1.0.3-1}
  
  #OS version to use. Also: centos5; suse11
  OS_VERSION=centos6
  HADOOP=hadoop-${HADOOP_VERSION}
  HADOOP_HOME=/usr/lib/hadoop
  #HADOOP_CONF_DIR=$HADOOP_HOME/conf
  HADOOP_CONF_DIR=/etc/hadoop/conf.whirr

  HADOOP_PACKAGE="hadoop hadoop-native hadoop-pipes hadoop-libhdfs  snappy snappy-devel snappy.i686 snappy-devel.i686  openssl hadoop-lzo lzo lzo-devel hadoop-lzo-native "

  echo "about to install $HADOOP_PACKAGE from HDP release $HDP_VERSION"

  register_hortonworks_repo
  
#  if which dpkg &> /dev/null; then
#    retry_apt_get update
#    retry_apt_get -y install $HADOOP_PACKAGE
#    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
#    update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
#  elif which rpm &> /dev/null; then
  echo "about to install $HADOOP_PACKAGE"
  retry_yum install -y $HADOOP_PACKAGE
  if ! retry_yum install -y $HADOOP_PACKAGE;
  then
    echo "Failed to Install $HADOOP_PACKAGE from yum"
    exit 1;
  fi
  
  #copy conf.empty to the new dir
  
  if ! cp -r /etc/hadoop/conf.empty $HADOOP_CONF_DIR;
  then
    echo "Failed to create configuration dir $HADOOP_CONF_DIR"
    exit 1;
  fi
  # and switch over to it
  echo "Hadoop installed: switching to version in $HADOOP_CONF_DIR"
  alternatives --install /etc/hadoop/conf hadoop-conf $HADOOP_CONF_DIR 90
#  fi
  
  #Add a group. In the absence of an easy way to see if the group is there,
  #the error code 9, "duplicate group name" is taken as a sign of existence,
  #so making this operation idempotent
#  groupadd -r hadoop
#  retval=$?
#  if ((${retval} != 0 && ${retval} != 9))
#  then
#    echo "adding group hadoop failed with return code ${retval}"
#    exit 1;
#  fi

  #  set up user accounts
#hadoop
#mapred:x:105:160:Hadoop MapReduce:/usr/lib/hadoop:/bin/bash
#hdfs:x:106:159:Hadoop HDFS:/usr/lib/hadoop:/bin/bash


#This is what the Hadoop RPM does
#getent group hadoop 2>/dev/null >/dev/null || /usr/sbin/groupadd -g 123 -r hadoop
#
#/usr/sbin/useradd --comment "Hadoop MapReduce" -u 202 --shell /bin/bash -M -r -g hadoop --home /tmp mapred 2> /dev/null || :
#/usr/sbin/useradd --comment "Hadoop HDFS" -u 201 --shell /bin/bash -M -r -g hadoop --home /tmp hdfs 2> /dev/null || :

#  if ! id hadoop ; then
#    if ! useradd -c "Hadoop" --system --user-group --home /usr/lib/hadoop -M hadoop;
#    then
#      echo "Failed to add user hadoop"
#      exit 1;
#    else 
#      echo "created user Hadoop"
#    fi
#  fi


#Create the groups
getent group hadoop >/dev/null || groupadd -r hadoop
getent group hdfs >/dev/null   || groupadd -r hdfs
getent group mapred >/dev/null || groupadd -r mapred

# Create a mapred user if one does not already exist.

getent passwd mapred >/dev/null  \
  || /usr/sbin/useradd --comment "Hadoop MapReduce" --shell /bin/bash -M -r -g mapred -G hadoop --home %{lib_hadoop} mapred \
  || exit 1

# Create an hdfs user if one does not already exist.
getent passwd hdfs >/dev/null \
  || /usr/sbin/useradd --comment "Hadoop HDFS" --shell /bin/bash -M -r -g hdfs -G hadoop --home %{lib_hadoop} hdfs \
  || exit 1


# symlink snappy in 
ln -sf /usr/lib64/libsnappy.so /usr/lib/hadoop/lib/native/Linux-amd64-64/.

#ln -sf /usr/lib/libsnappy.so /usr/lib/hadoop/lib/native/Linux-i386-32/.

  echo "Hadoop RPM installation and user configuration complete"
  INSTALL_HADOOP_DONE=1
}
