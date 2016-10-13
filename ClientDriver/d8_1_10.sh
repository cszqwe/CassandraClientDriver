#!/bin/bash
for i in {1..10}
do
	time java -cp target/my-app2-1.0-SNAPSHOT-jar-with-dependencies.jar  clientdriver.ClientDriver d8cas ~/cs4224/test2.txt 127.0.0.1 >$i.txt &
#	java -cp target/my-app2-1.0-SNAPSHOT-jar-with-dependencies.jar  clientdriver.ClientDriver d8cas ~/cs4224/test.txt 127.0.0.1 >2.txt &
done