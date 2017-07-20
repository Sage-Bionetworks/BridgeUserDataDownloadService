package org.sagebionetworks.bridge.udd.worker;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.json.DefaultObjectMapper;

@SuppressWarnings("unchecked")
public class BridgeUddRequestTest {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void nullStudyId() {
        new BridgeUddRequest.Builder().withUsername("test-username").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*studyId.*")
    public void emptyStudyId() {
        new BridgeUddRequest.Builder().withStudyId("").withUsername("test-username")
                .withStartDate(LocalDate.parse("2015-08-15")).withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*username.*")
    public void nullUsername() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withStartDate(LocalDate.parse("2015-08-15"))
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*username.*")
    public void emptyUsername() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("")
                .withStartDate(LocalDate.parse("2015-08-15")).withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*startDate.*")
    public void nullStartDate() {
        new BridgeUddRequest.Builder().withStudyId("test-study").withUsername("test-username")
                .withEndDate(LocalDate.parse("2015-08-19")).build();
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*endDate.*")
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
    public void withUserId() {
        BridgeUddRequest request = new BridgeUddRequest.Builder().withStudyId("test-study").withUserId("test-user-id")
                .withStartDate(LocalDate.parse("2015-08-15")).withEndDate(LocalDate.parse("2015-08-19")).build();
        assertEquals(request.getStudyId(), "test-study");
        assertEquals(request.getUserId(), "test-user-id");
        assertEquals(request.getStartDate().toString(), "2015-08-15");
        assertEquals(request.getEndDate().toString(), "2015-08-19");
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
        BridgeUddRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeUddRequest.class);
        assertEquals(request.getStudyId(), "json-study");
        assertEquals(request.getUsername(), "json-user");
        assertEquals(request.getStartDate().toString(), "2015-08-03");
        assertEquals(request.getEndDate().toString(), "2015-08-07");

        // convert back to JSON
        String convertedJson =DefaultObjectMapper.INSTANCE.writeValueAsString(request);

        // then convert to a map so we can validate the raw JSON
        Map<String, String> jsonMap = DefaultObjectMapper.INSTANCE.readValue(convertedJson, Map.class);
        assertEquals(4, jsonMap.size());
        assertEquals(jsonMap.get("studyId"), "json-study");
        assertEquals(jsonMap.get("username"), "json-user");
        assertEquals(jsonMap.get("startDate"), "2015-08-03");
        assertEquals(jsonMap.get("endDate"), "2015-08-07");
    }

    @Test
    public void jsonSerializationWithUserId() throws Exception {
        // start with JSON
        String jsonText = "{\n" +
                "   \"studyId\":\"json-study\",\n" +
                "   \"userId\":\"json-user-id\",\n" +
                "   \"startDate\":\"2015-08-03\",\n" +
                "   \"endDate\":\"2015-08-07\"\n" +
                "}";

        // convert to POJO
        BridgeUddRequest request = DefaultObjectMapper.INSTANCE.readValue(jsonText, BridgeUddRequest.class);
        assertEquals(request.getStudyId(), "json-study");
        assertEquals(request.getUserId(), "json-user-id");
        assertEquals(request.getStartDate().toString(), "2015-08-03");
        assertEquals(request.getEndDate().toString(), "2015-08-07");

        // convert back to JSON
        JsonNode jsonNode = DefaultObjectMapper.INSTANCE.convertValue(request, JsonNode.class);
        assertEquals(4, jsonNode.size());
        assertEquals(jsonNode.get("studyId").textValue(), "json-study");
        assertEquals(jsonNode.get("userId").textValue(), "json-user-id");
        assertEquals(jsonNode.get("startDate").textValue(), "2015-08-03");
        assertEquals(jsonNode.get("endDate").textValue(), "2015-08-07");
    }
}
