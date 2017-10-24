#!/bin/bash

# run N slave containers
N=$1

if [ $# != 1  ]
then
	echo "Set first parametar as number of nodes"
	exit 1
fi

# delete old master container and start new master container
docker rm -f master.krejcmat.com &> /dev/null
echo "start master container..."
docker run -d -t --restart=always --dns 127.0.0.1 -p 9090:9090 -P --name master.krejcmat.com -h master.krejcmat.com -w /root krejcmat/hadoop-hbase-master&> /dev/null

# get the IP address of master container
FIRST_IP=$(docker inspect --format="{{.NetworkSettings.IPAddress}}" master.krejcmat.com)

# delete old slave containers and start new slave containers
for i in `seq $N`
do
	docker rm -f slave$i.krejcmat.com &> /dev/null
	echo "start slave$i container..."
	docker run -d -t --restart=always --dns 127.0.0.1 -P --name slave$i.krejcmat.com -h slave$i.krejcmat.com -e JOIN_IP=$FIRST_IP krejcmat/hadoop-hbase-slave &> /dev/null
done


# create a new Bash session in the master container
docker exec -it master.krejcmat.com bash
