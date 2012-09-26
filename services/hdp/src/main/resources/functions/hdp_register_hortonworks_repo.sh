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


# see http://bit.ly/QcFWqv 
# http://docs.hortonworks.com/CURRENT/index.htm#Deploying_Hortonworks_Data_Platform/Using_gsInstaller/System_Requirements_For_Test_And_Production_Clusters.htm

# the layout is OS-specific; this script only supports Centos6, unless OS_VERSION is changed,
#
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/centos5/hdp.repo
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/centos6/hdp.repo
#http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/suse11/hdp.repo
# RPM key looks like
#   http://public-repo-1.hortonworks.com/HDP-1.1.0.15/repos/centos6/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins

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
  retry_yum update -y
  REGISTER_HORTONWORKS_REPO=1
}

