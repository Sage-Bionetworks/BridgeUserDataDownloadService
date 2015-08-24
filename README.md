# BridgeUserDataDownloadService
Bridge User Data Download (BUDD) Service

Pre-reqs:
Java Cryptography Extensions - http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html
(if using Oracle JDK, not required if using Open JDK)

Set-up:
In your home directory, add a file BridgeUserDataDownloadService.conf and add username, health code key, and stormpath
ID and secret. See main/resources/BridgeUserDatadownloadService.conf for an example. (Note that any attribute you don't
add to your local conf file will fall back to the bundled conf file.)

To run a full build (including compile, unit tests, findbugs, and jacoco test coverage), run:
mvn verify

(A full build takes about 20 sec on my laptop, from a clean workspace.)

To just run findbugs, run:
mvn compile findbugs:check

To run findbugs and get a friendly GUI to read about the bugs, run:
mvn compile findbugs:findbugs findbugs:gui

To run jacoco coverage reports and checks, run:
mvn test jacoco:report jacoco:check

Jacoco report will be in target/site/jacoco/index.html

To run this locally, run
mvn spring-boot:run


Useful Spring Boot / Maven development resouces:
http://stackoverflow.com/questions/27323104/spring-boot-and-maven-exec-plugin-issue
http://techblog.molindo.at/2007/11/maven-unable-to-find-resources-in-test-cases.html
