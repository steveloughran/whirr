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
function configure_hdp_hadoop() {
  local OPTIND
  local OPTARG
  
  if [ "${CONFIGURE_HADOOP_DONE}" == "1" ]; then
    echo "Hadoop is already configured."
    return;
  fi
  
  ROLES=$1
  shift
  
  
  HADOOP_HOME=/usr/lib/hadoop
  HADOOP_CONF_DIR=/etc/hadoop/conf
  
  make_hadoop_dirs /data*

  # Copy generated configuration files in place
  cp /tmp/{core,hdfs,mapred}-site.xml $HADOOP_CONF_DIR
  cp /tmp/hadoop-env.sh $HADOOP_CONF_DIR
  cp /tmp/hadoop-metrics.properties $HADOOP_CONF_DIR

  # Keep PID files in a non-temporary directory
#  HADOOP_PID_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_PID_DIR)
#  HADOOP_PID_DIR=${HADOOP_PID_DIR:-/var/run/hadoop}
#  mkdir -p $HADOOP_PID_DIR
#  chgrp -R hadoop $HADOOP_PID_DIR
#  chmod -R g+w $HADOOP_PID_DIR
#
#  HDFS_PID_DIR=$HADOOP_PID_DIR/hdfs
#  mkdir -p $HDFS_PID_DIR
#  chgrp -R hadoop $HDFS_PID_DIR
#  chmod -R g+w $HDFS_PID_DIR
#  chown -R hdfs $HDFS_PID_DIR
#
#  MR_PID_DIR=$HADOOP_PID_DIR/hdfs
#  mkdir -p $MR_PID_DIR
#  chgrp -R mapred $MR_PID_DIR
#  chmod -R g+w $MR_PID_DIR
#  chown -R mapred $MR_PID_DIR

  # Create the actual log dir
#  mkdir -p /data/hadoop/logs
#  chgrp -R hadoop /data/hadoop/logs
#  chmod -R g+w /data/hadoop/logs

  # Create a symlink at $HADOOP_LOG_DIR
#  HADOOP_LOG_DIR=$(. /tmp/hadoop-env.sh; echo $HADOOP_LOG_DIR)
#  HADOOP_LOG_DIR=${HADOOP_LOG_DIR:-/var/log/hadoop/logs}
#  rm -rf $HADOOP_LOG_DIR
#  mkdir -p $(dirname $HADOOP_LOG_DIR)
#  ln -s /data/hadoop/logs $HADOOP_LOG_DIR
#  chgrp -R hadoop $HADOOP_LOG_DIR
#  chmod -R g+w $HADOOP_LOG_DIR

  echo "Roles=$ROLES"
  if [ $(echo "$ROLES" | grep "hadoop-namenode" | wc -l) -gt 0 ]; then
    echo "Starting Namenode"
    start_namenode
  fi
  
  for role in $(echo "$ROLES" | tr "," "\n"); do
    case $role in
    hadoop-secondarynamenode)
      echo "Starting Secondary Namenode"
      start_hadoop_daemon hadoop-secondarynamenode
      ;;
    hadoop-jobtracker)
      echo "Starting Job Tracker"
      start_hadoop_daemon hadoop-jobtracker
      ;;
    hadoop-datanode)
      echo "Starting Datanode"
      start_hadoop_daemon hadoop-datanode
      ;;
    hadoop-tasktracker)
      echo "Starting Task Tracker"
      start_hadoop_daemon hadoop-tasktracker
      ;;
    esac
  done
  
    CONFIGURE_HADOOP_DONE=1
  
}

function make_hadoop_dirs {
  for mount in "$@"; do
    if [ ! -e $mount/hadoop ]; then
      mkdir -p $mount/hadoop
      chgrp -R hadoop $mount/hadoop
      chmod -R g+w $mount/hadoop
    fi
    if [ ! -e $mount/tmp ]; then
      mkdir $mount/tmp
      chmod a+rwxt $mount/tmp
    fi
  done
}

function start_namenode() {
#  if which dpkg &> /dev/null; then
#    retry_apt_get -y install $HDFS_PACKAGE_PREFIX-namenode
#    AS_HDFS="su -s /bin/bash - hdfs -c"
#    # Format HDFS
#    [ ! -e /data/hadoop/hdfs ] && $AS_HDFS "$HADOOP namenode -format"
#  elif which rpm &> /dev/null; then
    echo "installing hadoop-namenode"
    retry_yum install -y hadoop-namenode
    AS_HDFS="/sbin/runuser -s /bin/bash - hdfs -c"
    
    BIN_HADOOP="$HADOOP_HOME/bin/hadoop"
    
    # Format HDFS
    echo "formatting HDFS"
    [ ! -e /data/hadoop/hdfs ] && $AS_HDFS "$BIN_HADOOP namenode -format"
#  fi

  echo "starting service"

  service hadoop-namenode start

  echo "Creating initial HDFS structure"

  $AS_HDFS "$BIN_HADOOP dfsadmin -safemode wait"
  $AS_HDFS "$BIN_HADOOP fs -mkdir /user"
  # The following is questionable, as it allows a user to delete another user
  # It's needed to allow users to create their own user directories
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /user"
  $AS_HDFS "$BIN_HADOOP fs -mkdir /hadoop"
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /hadoop"
  $AS_HDFS "$BIN_HADOOP fs -mkdir /hbase"
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /hbase"
  $AS_HDFS "$BIN_HADOOP fs -mkdir /mnt"
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /mnt"

  # Create temporary directory for Pig and Hive in HDFS
  $AS_HDFS "$BIN_HADOOP fs -mkdir /tmp"
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /tmp"
  $AS_HDFS "$BIN_HADOOP fs -mkdir /user/hive/warehouse"
  $AS_HDFS "$BIN_HADOOP fs -chmod +w /user/hive/warehouse"
}

#Start the hadoop daemon -failure to do so triggers a script failure
function start_hadoop_daemon() {
  local daemon
  local retval
  daemon=$1
  echo "Installing $daemon"
#  if which dpkg &> /dev/null; then
#    retry_apt_get -y install $daemon
#  elif which rpm &> /dev/null; then
    retry_yum install -y $daemon
#  fi
  echo "Starting $daemon"
  service $daemon start
  retval=$?
  if ((${retval} == 0))
  then
    echo "Service $daemon is started"
  else
    ocf_log err "Service $daemon failed with return code ${retval}"
    exit 1;
  fi
}

