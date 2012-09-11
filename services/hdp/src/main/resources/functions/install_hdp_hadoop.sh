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
function register_hortonworks_repo() {

#  if which dpkg &> /dev/null; then
#      cat > /etc/apt/sources.list.d/cloudera-$REPO.list <<EOF
#deb http://$REPO_HOST/debian lucid-$REPO contrib
#deb-src http://$REPO_HOST/debian lucid-$REPO contrib
#EOF
#      curl -s http://$REPO_HOST/debian/archive.key | apt-key add -
#    retry_apt_get -y update
#  elif which rpm &> /dev/null; then
  rm -f /etc/yum.repos.d/hdp*.repo
  local REPOFILE=/etc/yum.repos.d/hdp-whirr-${REPO}-${HDP_VERSION}.repo
  local baseurl="http://${REPO_HOST}/HDP-${HDP_VERSION}/repos/centos5"
  cat > $REPOFILE << EOF
[hdp-${REPO}]
name=Hortonworks Data Platform Version - HDP-${HDP_VERSION}
baseurl=${baseurl}
gpgcheck=0
enabled=1
priority=1
#gpgkey = http://$REPO_HOST/redhat/TBD

EOF

  echo "installed new repo file ${REPOFILE} with repository ${baseurl}"
  
  echo "About to update yum"
  retry_yum update -y retry_yum
#  fi
}

function install_hdp_hadoop() {
  local OPTIND
  local OPTARG
  HDP_VERSION=1.0.0.12
  REPO=${REPO:-hdp1}
  REPO_HOST=${REPO_HOST:-public-repo-1.hortonworks.com}
  HADOOP_VERSION=${HADOOP_VERSION:-1.0.3-1}
  HADOOP=hadoop-${HADOOP_VERSION}
  HADOOP_HOME=/usr/lib/hadoop
  #HADOOP_CONF_DIR=$HADOOP_HOME/conf
  HADOOP_CONF_DIR=/etc/hadoop/conf.whirr

# package version to use w/ yum
  HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}

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
  
  #copy conf.empty to the new dir
  cp -r /etc/hadoop/conf.empty $HADOOP_CONF_DIR
  
  # and switch over to it
  echo "Hadoop installed: switching to version in $HADOOP_CONF_DIR"
  alternatives --install /etc/hadoop/conf hadoop-conf $HADOOP_CONF_DIR 90
#  fi
  
  INSTALL_HADOOP_DONE=1
}
