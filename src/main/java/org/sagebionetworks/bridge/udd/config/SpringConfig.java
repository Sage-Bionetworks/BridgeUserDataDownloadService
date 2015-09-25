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
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.stormpath.sdk.api.ApiKey;
import com.stormpath.sdk.api.ApiKeys;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.client.Clients;
import org.sagebionetworks.client.SynapseAdminClientImpl;
import org.sagebionetworks.client.SynapseClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.config.PropertiesConfig;
import org.sagebionetworks.bridge.crypto.AesGcmEncryptor;
import org.sagebionetworks.bridge.crypto.Encryptor;
import org.sagebionetworks.bridge.udd.s3.S3Helper;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.udd")
@Configuration
public class SpringConfig {
    @Bean(name = "auxiliaryExecutorService")
    public ExecutorService auxiliaryExecutorService() {
        return Executors.newFixedThreadPool(environmentConfig().getInt("threadpool.aux.count"));
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean(name = "ddbPrefix")
    public String ddbPrefix() {
        Config envConfig = environmentConfig();
        String envName = envConfig.getEnvironment().name().toLowerCase();
        String userName = envConfig.getUser();
        return envName + '-' + userName + '-';
    }

    @Bean(name = "ddbHealthIdTable")
    public Table ddbHealthIdTable() {
        return ddbClient().getTable(ddbPrefix() + "HealthId");
    }

    @Bean(name = "ddbStudyTable")
    public Table ddbStudyTable() {
        return ddbClient().getTable(ddbPrefix() + "Study");
    }

    @Bean(name = "ddbSynapseMapTable")
    public Table ddbSynapseMapTable() {
        return ddbClient().getTable(environmentConfig().get("synapse.map.table"));
    }

    @Bean(name = "ddbSynapseSurveyTable")
    public Table ddbSynapseSurveyTable() {
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

    private static final String CONFIG_FILE = "BridgeUserDataDownloadService.conf";
    private static final String DEFAULT_CONFIG_FILE = CONFIG_FILE;
    private static final String USER_CONFIG_FILE = System.getProperty("user.home") + "/" + CONFIG_FILE;

    @Bean
    public Config environmentConfig() {
        String defaultConfig = getClass().getClassLoader().getResource(DEFAULT_CONFIG_FILE).getPath();
        Path defaultConfigPath = Paths.get(defaultConfig);
        Path localConfigPath = Paths.get(USER_CONFIG_FILE);

        try {
            if (Files.exists(localConfigPath)) {
                return new PropertiesConfig(defaultConfigPath, localConfigPath);
            } else {
                return new PropertiesConfig(defaultConfigPath);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Bean(name = "healthCodeEncryptor")
    public Encryptor healthCodeEncryptor() throws IOException {
        // TODO: BridgePF supports multiple versions of this. However, this will require a code change anyway. We need
        // to refactor these into a shared library anyway, so for the initial investment, do something quick and dirty
        // until we have the resources to do the refactor properly.
        return new AesGcmEncryptor(environmentConfig().get("health.code.key"));
    }

    @Bean
    public AmazonS3Client s3Client() {
        return new AmazonS3Client();
    }

    @Bean
    public S3Helper s3Helper() {
        S3Helper s3Helper = new S3Helper();
        s3Helper.setS3Client(s3Client());
        return s3Helper;
    }

    @Bean
    public AmazonSimpleEmailServiceClient sesClient() {
        return new AmazonSimpleEmailServiceClient();
    }

    @Bean
    public Client stormpathClient() throws IOException {
        Config envConfig = environmentConfig();

        ApiKey apiKey = ApiKeys.builder().setId(envConfig.get("stormpath.id"))
                .setSecret(envConfig.get("stormpath.secret")).build();
        return Clients.builder().setApiKey(apiKey).setBaseUrl("https://enterprise.stormpath.io/v1").build();
    }

    @Bean
    public AmazonSQSClient sqsClient() {
        return new AmazonSQSClient();
    }

    @Bean
    public SynapseClient synapseClient() {
        Config envConfig = environmentConfig();

        SynapseClient synapseClient = new SynapseAdminClientImpl();
        synapseClient.setUserName(envConfig.get("synapse.user"));
        synapseClient.setApiKey(envConfig.get("synapse.api.key"));
        return synapseClient;
    }
}
