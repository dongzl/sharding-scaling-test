db_host='127.0.0.1'
db_port='3306'
db_user='root'
db_passwd='123456'

mvn package -Dmaven.test.skip=true
cd sharding-scaling-bootstrap/target
rm -rf sharding-scaling-bootstrap-1.0.0-SNAPSHOT
unzip sharding-scaling-bootstrap-1.0.0-SNAPSHOT-bin.zip
cd ../../
cp mysql-connector-java-6.0.6.jar sharding-scaling-bootstrap/target/sharding-scaling-bootstrap-1.0.0-SNAPSHOT/lib
mysql -h$db_host -u$db_user -p$db_passwd < mysql_ddl.sql
python3 generate_mysql_data.py $db_host $db_port $db_user $db_passwd test t1,t2,t3 5
python3 generate_mysql_data.py $db_host $db_port $db_user $db_passwd test t1,t2,t3 10 &
sharding-scaling-bootstrap/target/sharding-scaling-bootstrap-1.0.0-SNAPSHOT/bin/start.sh \
  scaling \
  --input-sharding-config conf/config-sharding.yaml \
  --output-jdbc-url "jdbc:mysql://$db_host:$db_port/test2?useSSL=false" \
  --output-jdbc-username $db_user \
  --output-jdbc-password $db_passwd
#mysql -h127.0.0.1 -uroot -p123456 < mysql_check.sql