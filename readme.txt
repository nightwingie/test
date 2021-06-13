mvn clean install

mvn exec:java -Dexec.mainClass=com.test.log.LogProcessor -Dexec.classpathScope=test -Dexec.args="logfile.txt"

