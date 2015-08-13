package org.sagebionetworks.bridge.udd.config;

import java.io.File;
import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.JsonNode;
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
import org.sagebionetworks.bridge.udd.util.BridgeUddUtil;

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

    // TODO: move this to something other than a JSON file in your home directory
    @Bean
    public JsonNode configAttributes() throws IOException {
        File configFile = new File(System.getProperty("user.home") + "/bridge-udd-config.json");
        return BridgeUddUtil.JSON_OBJECT_MAPPER.readTree(configFile);
    }

    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
    }

    @Bean(name = "ddbHealthIdTable")
    public Table ddbHealthIdTable() {
        // TODO: move table prefix to config
        return ddbClient().getTable("local-DwayneJeng-HealthId");
    }

    @Bean(name = "ddbStudyTable")
    public Table ddbStudyTable() {
        // TODO: move table prefix to config
        return ddbClient().getTable("local-DwayneJeng-Study");
    }

    @Bean(name = "ddbUploadTable")
    public Table ddbUploadTable() {
        // TODO: move table prefix to config
        return ddbClient().getTable("local-DwayneJeng-Upload2");
    }

    @Bean(name = "ddbUploadTableIndex")
    public Index ddbUploadTableIndex() {
        return ddbUploadTable().getIndex("healthCode-uploadDate-index");
    }

    @Bean(name = "healthCodeEncryptor")
    public AesGcmEncryptor healthCodeEncryptor() throws IOException {
        // TODO: BridgePF supports multiple versions of this. However, this will require a code change anyway. We need
        // to refactor these into a shared library anyway, so for the initial investment, do something quick and dirty
        // until we have the resources to do the refactor properly.
        return new AesGcmEncryptor(configAttributes().get("health-code-key").textValue());
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
        JsonNode configAttrs = configAttributes();

        ApiKey apiKey = ApiKeys.builder().setId(configAttrs.get("stormpath-id").textValue())
                .setSecret(configAttrs.get("stormpath-secret").textValue()).build();
        return Clients.builder().setApiKey(apiKey).setBaseUrl("https://enterprise.stormpath.io/v1").build();
    }

    @Bean
    public AmazonSQSClient sqsClient() {
        return new AmazonSQSClient();
    }
}
