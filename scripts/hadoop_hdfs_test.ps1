# you need to install python3, docker & docker-compose
# same as the script executed by github actions
# run from root dir cluster-pack scripts/hadoop_hdfs_tests.ps1
pip install hadoop-test-cluster
htcluster startup --image cdh5 --mount .:cluster-pack
# to avoid sharing files on the worker node we copy the python install script via hdfs to worker /tmp folder
htcluster exec -u root -s edge -- chown -R testuser /home/testuser
htcluster exec -u root -s edge -- /home/testuser/cluster-pack/tests/integration/install_python.sh
htcluster exec -u root -s edge -- hdfs dfs -put /home/testuser/cluster-pack/tests/integration/install_python.sh hdfs:///tmp
htcluster exec -u root -s worker -- hdfs dfs -get hdfs:///tmp/install_python.sh ~
htcluster exec -u root -s worker -- ./install_python.sh
htcluster exec -s edge -- /home/testuser/cluster-pack/tests/integration/hadoop_hdfs_tests.sh
