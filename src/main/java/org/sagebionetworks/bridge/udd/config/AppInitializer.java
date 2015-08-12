package org.sagebionetworks.bridge.udd.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.web.SpringBootServletInitializer;

// Running locally will work fine without SpringBootServletInitializer, because mvn spring-boot:run automatically
// creates a Tomcat container. However, Elastic Beanstalk needs the SpringBootServletInitializer so it can initialize
// against Elastic Beanstalk's pre-existing Tomcat installation.
@SpringBootApplication
public class AppInitializer extends SpringBootServletInitializer {
    public static void main(String[] args) {
        SpringApplication.run(AppInitializer.class, args);
    }
}
