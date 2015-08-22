package org.sagebionetworks.bridge.udd.worker;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.util.BridgeUddUtil;

@SuppressWarnings("unchecked")
public class BridgeUddRequestTest {
    @Test(expectedExceptions = IllegalStateException.class)
    public void nullStudyId() {
        new BridgeUddRequest.Builder().withUsername("test-username").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void emptyStudyId() {
        new BridgeUddRequest.Builder().withStudyId("").withUsername("test-username")
                .withStartDate(LocalDate.parse("2015-08-15")).withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void nullUsername() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void emptyUsername() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("")
                .withStartDate(LocalDate.parse("2015-08-15")).withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void nullStartDate() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("test-username")
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void nullEndDate() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("test-username")
                .withStartDate(LocalDate.parse("2015-08-15")).build();
    }

    @Test
    public void startDateBeforeEndDate() {
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId("test-study")
                .withUsername("test-username").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();
        assertEquals(request.getStudyId(), "test-study");
        assertEquals(request.getUsername(), "test-username");
        assertEquals(request.getStartDate().toString(), "2015-08-15");
        assertEquals(request.getEndDate().toString(), "2015-08-19");
    }

    @Test
    public void startDateSameAsEndDate() {
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId("test-study")
                .withUsername("test-username").withStartDate(LocalDate.parse("2015-08-17"))
                .withEndDate(LocalDate.parse("2015-08-17")).build();
        assertEquals(request.getStudyId(), "test-study");
        assertEquals(request.getUsername(), "test-username");
        assertEquals(request.getStartDate().toString(), "2015-08-17");
        assertEquals(request.getEndDate().toString(), "2015-08-17");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void startDateAfterEndDate() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("test-username")
                .withStartDate(LocalDate.parse("2015-08-16")).withEndDate(LocalDate.parse("2015-08-14")).build();
    }

    @Test
    public void jsonSerialization() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"json-study\",\n" +
                "   \"username\":\"json-user\",\n" +
                "   \"startDate\":\"2015-08-03\",\n" +
                "   \"endDate\":\"2015-08-07\"\n" +
                "}";

        // convert to POJO
        BridgeUddRequest request = BridgeUddUtil.JSON_OBJECT_MAPPER.readValue(jsonText, BridgeUddRequest.class);
        assertEquals(request.getStudyId(), "json-study");
        assertEquals(request.getUsername(), "json-user");
        assertEquals(request.getStartDate().toString(), "2015-08-03");
        assertEquals(request.getEndDate().toString(), "2015-08-07");

        // convert back to JSON
        String convertedJson = BridgeUddUtil.JSON_OBJECT_MAPPER.writeValueAsString(request);

        // then convert to a map so we can validate the raw JSON
        Map<String, String> jsonMap = BridgeUddUtil.JSON_OBJECT_MAPPER.readValue(convertedJson, Map.class);
        assertEquals(4, jsonMap.size());
        assertEquals(jsonMap.get("studyId"), "json-study");
        assertEquals(jsonMap.get("username"), "json-user");
        assertEquals(jsonMap.get("startDate"), "2015-08-03");
        assertEquals(jsonMap.get("endDate"), "2015-08-07");
    }
}
