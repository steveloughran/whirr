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

function hdp_register_hortonworks_repo() {

  if [ "$REGISTER_HORTONWORKS_REPO" == "1" ]
  then
    echo "Hortonworks repo is already installed."
    return;
  fi

  local HDP_VERSION=1.1.0.15
  local REPO=${REPO:-HDP-1}
  local REPO_HOST=${REPO_HOST:-public-repo-1.hortonworks.com}
  local HADOOP_VERSION=${HADOOP_VERSION:-1.0.3-1}
  
  #OS version to use. Also: centos5; suse11
  OS_VERSION=centos6
  local REPOFILE=/etc/yum.repos.d/hdp.repo
  rm -f $REPOFILE
  local baseurl="http://${REPO_HOST}/HDP-${HDP_VERSION}/repos/${OS_VERSION}"
  local utilsurl="http://${REPO_HOST}/HDP-UTILS-${HDP_VERSION}/repos/${OS_VERSION}"
  local keyurl="${baseurl}/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins"
  cat > $REPOFILE << EOF
[HDP-${REPO}]
name=Hortonworks Data Platform Version - HDP-${HDP_VERSION}
baseurl=${baseurl}
enabled=1
priority=1
gpgcheck=1
gpgkey=${keyurl}

[HDP-UTILS-${REPO}]
name=Hortonworks Data Platform Utils Version - HDP-UTILS-${REPO}
baseurl=${utilsurl}
enabled=1
priority=1
gpgcheck=1
gpgkey=${keyurl}
EOF

  echo "installed new repo file ${REPOFILE} with repository ${baseurl} and ${utilsurl}"
  REGISTER_HORTONWORKS_REPO=1
}

function hdp_preflight_checks() {
 #any preflight checks. 
 
  # validate the hostname resolves and reaches itself.
  # if this fails Hadoop won't come up, so bail out early with some basic diagnostics
  
  local hostname=`hostname`
  if ! ping -c 5 $hostname
  then
    echo "This server does not know its own name, cannot resolve it via DNS or /etc/hosts -or cannot reach it"
    echo "It's apparent hostname is '$hostname'"
    echo 
    echo "/etc/hosts is "
    cat /etc/hosts
    echo 
    echo "/etc/resolv.conf is"
    cat /etc/resolv.conf
    echo 
    exit 1;
  fi

  #look for yum. This generally fails because
  #a debian image has been brought up instead
#  if ! `which yum`
#  then
#    echo "This server does not have yum installed"
#    echo "This installation requires it"
#    echo 
#    echo "Either this is not a RHEL-based system, or its path is corrupt "
#    echo "uname -a is"
#    uname -a
#    echo 
#    echo "PATH is $PATH"
#    echo 
#    exit 1;
#  fi
# 
}

function install_hdp_hadoop() {
  local OPTIND
  local OPTARG
  local retval
  if [ "$INSTALL_HADOOP_DONE" == "1" ]
  then
    echo "Hadoop is already installed."
    return;
  fi
  hdp_preflight_checks
  
  #OS version to use. Also: centos5; suse11
  HADOOP_HOME=/usr/lib/hadoop
  #HADOOP_CONF_DIR=$HADOOP_HOME/conf
  HADOOP_CONF_DIR=/etc/hadoop/conf.whirr

  HADOOP_PACKAGE="hadoop hadoop-native hadoop-pipes hadoop-libhdfs hadoop-lzo lzo lzo-devel hadoop-lzo-native "
#  SNAPPY_PACKAGE="snappy snappy-devel snappy.i686 snappy-devel.i686"
  SNAPPY_PACKAGE="snappy snappy-devel"
  OPENSSL_PACKAGE="openssl"

  hdp_register_hortonworks_repo
  
#  if which dpkg &> /dev/null; then
#    retry_apt_get update
#    retry_apt_get -y install $HADOOP_PACKAGE
#    cp -r /etc/$HADOOP/conf.empty $HADOOP_CONF_DIR
#    update-alternatives --install /etc/$HADOOP/conf $HADOOP-conf $HADOOP_CONF_DIR 90
#  elif which rpm &> /dev/null; then
  echo "about to install $HADOOP_PACKAGE"
  if ! retry_yum install -y $HADOOP_PACKAGE;
  then
    echo "Failed to Install $HADOOP_PACKAGE from yum"
    exit 1;
  fi

   retry_yum install -y $SNAPPY_PACKAGE || exit 1

  
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
# these tests are valid for both 64 bit and 32 bit versions

stat -L /usr/lib64/libsnappy.so && ln -sf /usr/lib64/libsnappy.so /usr/lib/hadoop/lib/native/Linux-amd64-64/.
stat -L /usr/lib/libsnappy.so  && ln -sf /usr/lib/libsnappy.so /usr/lib/hadoop/lib/native/Linux-i386-32/.

  echo "Hadoop RPM installation and user configuration complete"
  INSTALL_HADOOP_DONE=1
}
