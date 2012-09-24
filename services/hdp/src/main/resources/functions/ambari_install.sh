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

# this requires that install_ambari_rpms() has already set up
# the relevant env variables
#
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

function ambari_install() {
  local OPTIND
  local OPTARG
  local retval

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

  HADOOP_PACKAGE="epel-release pdsh hmc curl"

  echo "about to install $HADOOP_PACKAGE from HDP release $HDP_VERSION"

  register_hortonworks_repo

  echo "about to install $HADOOP_PACKAGE"
  retry_yum install -y $HADOOP_PACKAGE
  if ! retry_yum install -y $HADOOP_PACKAGE;
  then
    echo "Failed to Install $HADOOP_PACKAGE from yum"
    exit 1;
  fi
  INSTALL_AMBARI_DONE=1
}
