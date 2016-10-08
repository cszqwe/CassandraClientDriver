mvn assembly:assembly -DdescriptorId=jar-with-dependencies

java -cp target/my-app2-1.0-SNAPSHOT-jar-with-dependencies.jar  clientdriver.ClientDriver d8cas ~/cs4224/test.txt 127.0.0.1
