package org.sagebionetworks.bridge.udd.helper;

import java.net.URL;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import com.google.common.base.Strings;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;

@Component
public class SesHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SesHelper.class);

    // TODO: move these to config
    private static final String DEFAULT_FROM_ADDRESS = "Bridge (Sage Bionetworks) <support@sagebridge.org>";
    private static final String SUBJECT_TEMPLATE = "Your requested data from %s";
    private static final String BODY_TEMPLATE_HTML = "<html>\n" +
            "   <body>\n" +
            "       <p>To download your requested data, please click <a href=\"%s\">here</a>.</p>\n" +
            "       <p>This link will expire on %s.</p>\n" +
            "   </body>\n" +
            "</html>";
    private static final String BODY_TEMPLATE_TEXT = "To download your requested data, please click on the following link:\n" +
            "%s\n" +
            "\n" +
            "This link will expire on %s.";

    private AmazonSimpleEmailServiceClient sesClient;

    @Autowired
    public void setSesClient(AmazonSimpleEmailServiceClient sesClient) {
        this.sesClient = sesClient;
    }

    public void sendPresignedUrlToAccount(StudyInfo studyInfo, PresignedUrlInfo presignedUrlInfo,
            AccountInfo accountInfo) {
        // from address
        String studySupportEmail = studyInfo.getSupportEmail();
        String fromAddress = !Strings.isNullOrEmpty(studySupportEmail) ? studySupportEmail : DEFAULT_FROM_ADDRESS;

        // to address
        Destination destination = new Destination().withToAddresses(accountInfo.getEmailAddress());

        // subject
        String studyName = studyInfo.getName();
        String subjectStr = String.format(SUBJECT_TEMPLATE, studyName);
        Content subject = new Content(subjectStr);

        // body
        String presignedUrlStr = presignedUrlInfo.getUrl().toString();
        String expirationTimeStr = presignedUrlInfo.getExpirationTime().toString(ISODateTimeFormat.dateTime());
        String bodyHtmlStr = String.format(BODY_TEMPLATE_HTML, presignedUrlStr, expirationTimeStr);
        String bodyTextStr = String.format(BODY_TEMPLATE_TEXT, presignedUrlStr, expirationTimeStr);
        Body body = new Body().withHtml(new Content(bodyHtmlStr)).withText(new Content(bodyTextStr));

        // send message
        Message message = new Message(subject, body);
        SendEmailRequest sendEmailRequest = new SendEmailRequest(fromAddress, destination, message);
        SendEmailResult sendEmailResult = sesClient.sendEmail(sendEmailRequest);

        LOG.info("Sent email to SES with message ID " + sendEmailResult.getMessageId());
    }
}
