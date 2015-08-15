# BridgeUserDataDownloadService
Bridge User Data Download (BUDD) Service

Pre-reqs:
Java Cryptography Extensions - http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html
(if using Oracle JDK, not required if using Open JDK)

Set-up:
In your home directory, add a file BridgeUserDataDownloadService.conf and add username, health code key, and stormpath
ID and secret. See main/resources/BridgeUserDatadownloadService.conf for an example. (Note that any attribute you don't
add to your local conf file will fall back to the bundled conf file.

To run this locally, run
mvn spring-boot:run


Useful Spring Boot / Maven development resouces:
http://stackoverflow.com/questions/27323104/spring-boot-and-maven-exec-plugin-issue
http://techblog.molindo.at/2007/11/maven-unable-to-find-resources-in-test-cases.html
