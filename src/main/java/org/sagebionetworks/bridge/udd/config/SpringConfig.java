package org.sagebionetworks.bridge.udd.config;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.stormpath.sdk.api.ApiKey;
import com.stormpath.sdk.api.ApiKeys;
import com.stormpath.sdk.client.Client;
import com.stormpath.sdk.client.Clients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.sagebionetworks.bridge.udd.crypto.AesGcmEncryptor;
import org.sagebionetworks.bridge.udd.crypto.BcCmsEncryptor;
import org.sagebionetworks.bridge.udd.crypto.CmsEncryptorCacheLoader;
import org.sagebionetworks.bridge.udd.s3.S3Helper;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@ComponentScan("org.sagebionetworks.bridge.udd")
@Configuration
public class SpringConfig {
    @Bean(name = "cmsEncryptorCache")
    @Autowired
    public LoadingCache<String, BcCmsEncryptor> cmsEncryptorCache(CmsEncryptorCacheLoader cacheLoader) {
        return CacheBuilder.newBuilder().build(cacheLoader);
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean(name = "ddbPrefix")
    public String ddbPrefix() {
        EnvironmentConfig envConfig = environmentConfig();
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

    @Bean(name = "ddbUploadTable")
    public Table ddbUploadTable() {
        return ddbClient().getTable(ddbPrefix() + "Upload2");
    }

    @Bean(name = "ddbUploadTableIndex")
    public Index ddbUploadTableIndex() {
        return ddbUploadTable().getIndex("healthCode-uploadDate-index");
    }

    @Bean
    public EnvironmentConfig environmentConfig() {
        return new EnvironmentConfig();
    }

    @Bean(name = "healthCodeEncryptor")
    public AesGcmEncryptor healthCodeEncryptor() throws IOException {
        // TODO: BridgePF supports multiple versions of this. However, this will require a code change anyway. We need
        // to refactor these into a shared library anyway, so for the initial investment, do something quick and dirty
        // until we have the resources to do the refactor properly.
        return new AesGcmEncryptor(environmentConfig().getProperty("health.code.key"));
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
        EnvironmentConfig envConfig = environmentConfig();

        ApiKey apiKey = ApiKeys.builder().setId(envConfig.getProperty("stormpath.id"))
                .setSecret(envConfig.getProperty("stormpath.secret")).build();
        return Clients.builder().setApiKey(apiKey).setBaseUrl("https://enterprise.stormpath.io/v1").build();
    }

    @Bean
    public AmazonSQSClient sqsClient() {
        return new AmazonSQSClient();
    }
}
