package org.sagebionetworks.bridge.udd;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// These configs get credentials from the default credential chain. For developer desktops, this is ~/.aws/credentials.
// For EC2 instances, this happens transparently.
// See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html and
// http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html#set-up-creds for more info.
@Configuration
public class SpringConfig {
    @Bean
    public DynamoDB ddbClient() {
        return new DynamoDB(new AmazonDynamoDBClient());
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

    @Bean
    public AmazonSQSClient sqsClient() {
        return new AmazonSQSClient();
    }
}
