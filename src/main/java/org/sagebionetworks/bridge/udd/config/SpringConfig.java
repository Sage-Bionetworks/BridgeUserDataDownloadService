package org.sagebionetworks.bridge.udd.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.stormpath.sdk.api.ApiKey;
import com.stormpath.sdk.api.ApiKeys;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.client.Clients;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.crypto.AesGcmEncryptor;
import org.sagebionetworks.bridge.crypto.Encryptor;
import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.file.FileHelper;
import org.sagebionetworks.bridge.heartbeat.HeartbeatLogger;
import org.sagebionetworks.bridge.s3.S3Helper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import javax.annotation.PostConstruct;

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

    private Properties envConfig;
    private String ddbPrefix;

    @PostConstruct
    public void bridgeConfig() {
        // setup conf file to load attributes
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        Resource resource = new ClassPathResource(DEFAULT_CONFIG_FILE);

        try {
            envConfig = PropertiesLoaderUtils.loadProperties(resource);

            if (Files.exists(localConfigPath)) {
                Properties localProps = new Properties();
                localProps.load(Files.newBufferedReader(localConfigPath, StandardCharsets.UTF_8));
                envConfig = localProps;
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // setup ddbPrefix for other bean methods to use
        String envName = envConfig.getProperty("bridge.env").toLowerCase();
        String userName = envConfig.getProperty("bridge.user");
        this.ddbPrefix = envName + '-' + userName + '-';
    }

    @Bean(name = "auxiliaryExecutorService")
    public ExecutorService auxiliaryExecutorService() {
        return Executors.newFixedThreadPool(Integer.parseInt(envConfig.getProperty("threadpool.aux.count")));
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean(name = "ddbHealthIdTable")
    public Table ddbHealthIdTable() {
        return ddbClient().getTable(ddbPrefix + "HealthId");
    }

    @Bean
    public DynamoQueryHelper ddbQueryHelper() {
        return new DynamoQueryHelper();
    }

    @Bean(name = "ddbStudyTable")
    public Table ddbStudyTable() {
        return ddbClient().getTable(ddbPrefix + "Study");
    }

    @Bean(name = "ddbSynapseMapTable")
    public Table ddbSynapseMapTable() {
        return ddbClient().getTable(envConfig.getProperty(envConfig.getProperty("bridge.env") + ".synapse.map.table"));
    }

    // Naming note: This is a DDB table containing references to a set of Synapse tables. The name is a bit confusing,
    // but I'm not sure how to make it less confusing.
    @Bean(name = "ddbSynapseSurveyTablesTable")
    public Table ddbSynapseSurveyTablesTable() {
        return ddbClient().getTable(ddbPrefix + "SynapseSurveyTables");
    }

    @Bean(name = "ddbUploadSchemaTable")
    public Table ddbUploadSchemaTable() {
        return ddbClient().getTable(ddbPrefix + "UploadSchema");
    }

    @Bean(name = "ddbUploadSchemaStudyIndex")
    public Index ddbUploadSchemaStudyIndex() {
        return ddbUploadSchemaTable().getIndex("studyId-index");
    }

    @Bean
    public FileHelper fileHelper() {
        return new FileHelper();
    }

    @Bean(name = "healthCodeEncryptor")
    public Encryptor healthCodeEncryptor() throws IOException {
        // TODO: BridgePF supports multiple versions of this. However, this will require a code change anyway. We need
        // to refactor these into a shared library anyway, so for the initial investment, do something quick and dirty
        // until we have the resources to do the refactor properly.
        return new AesGcmEncryptor(envConfig.getProperty("health.code.key"));
    }

    @Bean
    public HeartbeatLogger heartbeatLogger() {
        HeartbeatLogger heartbeatLogger = new HeartbeatLogger();
        heartbeatLogger.setIntervalMinutes(Integer.parseInt(envConfig.getProperty("heartbeat.interval.minutes")));
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

    @Bean(name="uddStompath")
    public Client stormpathClient() throws IOException {
        ApiKey apiKey = ApiKeys.builder().setId(envConfig.getProperty("stormpath.id"))
                .setSecret(envConfig.getProperty("stormpath.secret")).build();
        return Clients.builder().setApiKey(apiKey).setBaseUrl("https://enterprise.stormpath.io/v1").build();
    }

    @Bean(name="uddSynapseClient")
    public SynapseClient synapseClient() {
        SynapseClient synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(envConfig.getProperty("synapse.user"));
        synapseClient.setApiKey(envConfig.getProperty("synapse.api.key"));
        return synapseClient;
    }
}
