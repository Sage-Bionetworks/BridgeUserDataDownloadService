package org.sagebionetworks.bridge.udd.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.sns.AmazonSNSClient;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.model.ClientInfo;
import org.sagebionetworks.bridge.rest.model.SignIn;
import org.sagebionetworks.bridge.s3.S3Helper;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.udd")
@Configuration("uddConfig")
public class SpringConfig {
    private static final String CONFIG_FILE = "BridgeUserDataDownloadService.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    @Bean
    public ClientManager bridgeClientManager() throws IOException {
        Config config = bridgeConfig();
        String study = config.get("bridge.worker.study");
        String email = config.get("bridge.worker.email");
        String password = config.get("bridge.worker.password");
        SignIn bridgeCredentials = new SignIn().appId(study).email(email).password(password);

        ClientInfo clientInfo = new ClientInfo().appName("BridgeUDD").appVersion(1);
        return new ClientManager.Builder().withClientInfo(clientInfo).withSignIn(bridgeCredentials).build();
    }

    @Bean(name = "uddConfigProperties")
    public Config bridgeConfig() {
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(DEFAULT_CONFIG_FILE, localConfigPath);
            } else {
                return new PropertiesConfig(DEFAULT_CONFIG_FILE);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean(name = "uddDdbPrefix")
    public String ddbPrefix() {
        Config envConfig = bridgeConfig();
        String envName = envConfig.getEnvironment().name().toLowerCase();
        String userName = envConfig.getUser();
        return envName + '-' + userName + '-';
    }

    @Bean(name = "auxiliaryExecutorService")
    public ExecutorService auxiliaryExecutorService() {
        return Executors.newFixedThreadPool(bridgeConfig().getInt("threadpool.aux.count"));
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean
    public DynamoQueryHelper ddbQueryHelper() {
        return new DynamoQueryHelper();
    }

    @Bean(name = "ddbStudyTable")
    public Table ddbStudyTable() {
        return ddbClient().getTable(ddbPrefix() + "Study");
    }

    @Bean(name = "ddbSynapseMapTable")
    public Table ddbSynapseMapTable() {
        return ddbClient().getTable(bridgeConfig().get("synapse.map.table"));
    }

    // Naming note: This is a DDB table containing references to a set of Synapse tables. The name is a bit confusing,
    // but I'm not sure how to make it less confusing.
    @Bean(name = "ddbSynapseSurveyTablesTable")
    public Table ddbSynapseSurveyTablesTable() {
        return ddbClient().getTable(ddbPrefix() + "SynapseSurveyTables");
    }

    @Bean(name = "ddbUploadSchemaTable")
    public Table ddbUploadSchemaTable() {
        return ddbClient().getTable(ddbPrefix() + "UploadSchema");
    }

    @Bean(name = "ddbUploadSchemaStudyIndex")
    public Index ddbUploadSchemaStudyIndex() {
        return ddbUploadSchemaTable().getIndex("studyId-index");
    }

    @Bean
    public HeartbeatLogger heartbeatLogger() throws IOException {
        HeartbeatLogger heartbeatLogger = new HeartbeatLogger();
        heartbeatLogger.setIntervalMinutes(bridgeConfig().getInt("heartbeat.interval.minutes"));
        return heartbeatLogger;
    }

    @Bean
    public S3Helper s3Helper() {
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(new AmazonS3Client());
        return s3Helper;
    }

    @Bean
    public AmazonSimpleEmailServiceClient sesClient() {
        return new AmazonSimpleEmailServiceClient();
    }
    
    @Bean
    public AmazonSNSClient snsClient() {
        return new AmazonSNSClient();
    }
    
}
