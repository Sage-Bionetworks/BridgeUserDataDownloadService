package org.sagebionetworks.bridge.udd.helper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.util.List;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import com.amazonaws.services.simpleemail.model.SendEmailResult;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.accounts.AccountInfo;
import org.sagebionetworks.bridge.udd.dynamodb.StudyInfo;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;

public class SesHelperTest {
    @Test
    public void test() throws Exception {
        // mock SES client
        SendEmailResult mockSesResult = new SendEmailResult().withMessageId("test-ses-message-id");
        ArgumentCaptor<SendEmailRequest> sesRequestCaptor = ArgumentCaptor.forClass(SendEmailRequest.class);
        AmazonSimpleEmailServiceClient mockSesClient = mock(AmazonSimpleEmailServiceClient.class);
        when(mockSesClient.sendEmail(sesRequestCaptor.capture())).thenReturn(mockSesResult);

        // set up test helper
        SesHelper sesHelper = new SesHelper();
        sesHelper.setSesClient(mockSesClient);

        // set up test inputs
        String dummyPresignedUrl = "http://www.example.com/";

        // We parse the expiration date from a string, then format it back into a string so we can make sure our tests
        // have the same formatting as the real code. Otherwise, predicting the formatting in the real email can get
        // kinda hairy.
        DateTime dummyExpirationDate = DateTime.parse("2015-08-22T14:00-07:00");
        String dummyExpirationDateStr = dummyExpirationDate.toString(ISODateTimeFormat.dateTime());

        StudyInfo studyInfo = new StudyInfo.Builder().withName("Test Study").withStudyId("test-study")
                .withStormpathHref("dummy-stormpath-href").withSupportEmail("support@sagebase.org").build();
        PresignedUrlInfo presignedUrlInfo = new PresignedUrlInfo.Builder().withUrl(new URL(dummyPresignedUrl))
                .withExpirationTime(dummyExpirationDate).build();
        AccountInfo accountInfo = new AccountInfo.Builder().withEmailAddress("dummy-email@example.com")
                .withHealthId("dummy-health-id").withUsername("dummy-username").build();

        // execute and validate
        sesHelper.sendPresignedUrlToAccount(studyInfo, presignedUrlInfo, accountInfo);

        SendEmailRequest sesRequest = sesRequestCaptor.getValue();
        assertEquals(sesRequest.getSource(), "support@sagebase.org");

        List<String> toAddressList = sesRequest.getDestination().getToAddresses();
        assertEquals(toAddressList.size(), 1);
        assertEquals(toAddressList.get(0), "dummy-email@example.com");

        // for the actual email, just validate that the body (both HTML and text versions) contain the link and the
        // expiration date
        Body emailBody = sesRequest.getMessage().getBody();
        String htmlEmail = emailBody.getHtml().getData();
        assertTrue(htmlEmail.contains(dummyPresignedUrl));
        assertTrue(htmlEmail.contains(dummyExpirationDateStr));

        String textEmail = emailBody.getText().getData();
        assertTrue(textEmail.contains(dummyPresignedUrl));
        assertTrue(textEmail.contains(dummyExpirationDateStr));
    }
}
