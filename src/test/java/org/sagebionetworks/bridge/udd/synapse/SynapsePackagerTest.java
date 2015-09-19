package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.Writer;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.LocalDate;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.config.Config;
import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.helper.InMemoryFileHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelper;
import org.sagebionetworks.bridge.udd.helper.ZipHelperTest;
import org.sagebionetworks.bridge.udd.s3.PresignedUrlInfo;
import org.sagebionetworks.bridge.udd.s3.S3Helper;
import org.sagebionetworks.bridge.udd.worker.BridgeUddRequest;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SynapsePackagerTest {
    private static final String DUMMY_USER_DATA_BUCKET = "dummy-user-data-bucket";
    private static final DateTime MOCK_NOW = DateTime.parse("2015-09-17T12:43:41-07:00");
    private static final String TEST_START_DATE = "2015-03-09";
    private static final String TEST_END_DATE = "2015-09-17";
    private static final String TEST_HEALTH_CODE = "test-health-code";
    private static final String TEST_MASTER_ZIP_FILE_PREFIX = "userdata-" + TEST_START_DATE + "-to-" +
            TEST_END_DATE + "-";
    private static final int URL_EXPIRATION_HOURS = 12;

    // study and username don't matter for this class, only start date and end date
    private static final BridgeUddRequest TEST_UDD_REQUEST = new BridgeUddRequest.Builder().withStudyId("dummy-study")
            .withUsername("dummy-user").withStartDate(LocalDate.parse(TEST_START_DATE))
            .withEndDate(LocalDate.parse(TEST_END_DATE)).build();

    private S3Helper mockS3Helper;
    private InMemoryFileHelper inMemoryFileHelper;
    private SynapsePackager packager;
    private byte[] s3FileBytes;

    @Test
    public void noFiles() throws Exception {
        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of("test-table-id",
                new SynapseTaskResultContent(null, null, null, null));
        setupPackager(synapseTableToSchema, synapseTableToResult, null);

        // execute and validate
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(synapseTableToSchema, TEST_HEALTH_CODE,
                TEST_UDD_REQUEST);
        assertNull(presignedUrlInfo);

        // validate S3 not called
        verifyZeroInteractions(mockS3Helper);
        assertNull(s3FileBytes);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void normalCase() throws Exception {
        // For full branch coverage, we need the following cases:
        // * task with no files
        // * task with CSV
        // * task with CSV and bulk download
        // * 2 tasks with errors (to make sure error messages are collated properly)

        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = new ImmutableMap.Builder()
                .put("no-file-table", mock(UploadSchema.class))
                .put("csv-only-table", mock(UploadSchema.class))
                .put("csv-and-bulk-download-table", mock(UploadSchema.class))
                .put("error-table-1", mock(UploadSchema.class))
                .put("error-table-2", mock(UploadSchema.class))
                .build();

        Map<String, SynapseTaskResultContent> synapseTableToResult = new ImmutableMap.Builder()
                .put("no-file-table", new SynapseTaskResultContent(null, null, null, null))
                .put("csv-only-table", new SynapseTaskResultContent("csv-only.csv", "csv-only dummy csv", null, null))
                .put("csv-and-bulk-download-table", new SynapseTaskResultContent("csv-and-bulk-download.csv",
                        "csv-and-bulk-download dummy csv", "csv-and-bulk-download.zip",
                        "csv-and-bulk-download dummy zip"))
                .build();

        Map<String, ExecutionException> synapseTableToException = new ImmutableMap.Builder()
                .put("error-table-1", new ExecutionException("test exception 1", null))
                .put("error-table-2", new ExecutionException("test exception 2", null))
                .build();

        setupPackager(synapseTableToSchema, synapseTableToResult, synapseTableToException);

        // mock pre-signed URL call
        ArgumentCaptor<DateTime> expirationTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
        when(mockS3Helper.generatePresignedUrl(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                expirationTimeCaptor.capture(), eq(HttpMethod.GET))).thenReturn(new URL("http://example.com/"));

        // execute and validate
        long expectedExpirationTimeMillis = MOCK_NOW.plusHours(URL_EXPIRATION_HOURS).getMillis();
        PresignedUrlInfo presignedUrlInfo = packager.packageSynapseData(synapseTableToSchema, TEST_HEALTH_CODE,
                TEST_UDD_REQUEST);
        assertEquals(presignedUrlInfo.getUrl().toString(), "http://example.com/");
        assertEquals(presignedUrlInfo.getExpirationTime().getMillis(), expectedExpirationTimeMillis);

        // validate uploaded S3 file
        Map<String, String> unzippedMap = ZipHelperTest.unzipHelper(s3FileBytes);
        assertEquals(unzippedMap.size(), 4);
        assertEquals(unzippedMap.get("csv-only.csv"), "csv-only dummy csv");
        assertEquals(unzippedMap.get("csv-and-bulk-download.csv"), "csv-and-bulk-download dummy csv");
        assertEquals(unzippedMap.get("csv-and-bulk-download.zip"), "csv-and-bulk-download dummy zip");

        // For the error log, instead of exact string matching, just make sure it contains our error messages.
        String errorLogContent = unzippedMap.get(SynapsePackager.ERROR_LOG_FILE_NAME);
        assertTrue(errorLogContent.contains("test exception 1"));
        assertTrue(errorLogContent.contains("test exception 2"));

        // validate expiration time
        DateTime expirationTime = expirationTimeCaptor.getValue();
        assertEquals(expirationTime.getMillis(), expectedExpirationTimeMillis);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void firstErrorCase() throws Exception {
        // Test getting an error on the first step. The easiest way to inject the exception is to spy the packager and
        // make initAsyncTasks throw a RuntimeException.

        // set up inputs
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));

        // set up mocks - We bypass most of the stuff in setupPackager()
        packager = spy(new SynapsePackager());
        doThrow(RuntimeException.class).when(packager).initAsyncTasks(same(synapseTableToSchema), eq(TEST_HEALTH_CODE),
                same(TEST_UDD_REQUEST), any(File.class));

        inMemoryFileHelper = new InMemoryFileHelper();
        packager.setFileHelper(inMemoryFileHelper);

        // execute
        Exception thrownEx = null;
        try {
            packager.packageSynapseData(synapseTableToSchema, TEST_HEALTH_CODE, TEST_UDD_REQUEST);
            fail("expected exception");
        } catch (RuntimeException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    @Test
    public void lastErrorCase() throws Exception {
        // Test getting an error on the last step (get pre-signed URL). This allows us to test full cleanup.

        // setup test
        // We don't care about data inside the schema. Use mock schemas.
        Map<String, UploadSchema> synapseTableToSchema = ImmutableMap.of("test-table-id", mock(UploadSchema.class));
        Map<String, SynapseTaskResultContent> synapseTableToResult = ImmutableMap.of("test-table-id",
                new SynapseTaskResultContent("csv.csv", "dummy csv content", "bulkdownload.zip",
                        "dummy bulk download content"));
        setupPackager(synapseTableToSchema, synapseTableToResult, null);

        // mock pre-signed URL call
        ArgumentCaptor<DateTime> expirationTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
        when(mockS3Helper.generatePresignedUrl(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                expirationTimeCaptor.capture(), eq(HttpMethod.GET))).thenThrow(AmazonClientException.class);

        // execute
        Exception thrownEx = null;
        try {
            packager.packageSynapseData(synapseTableToSchema, TEST_HEALTH_CODE, TEST_UDD_REQUEST);
            fail("expected exception");
        } catch (AmazonClientException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);

        // validate uploaded S3 file
        Map<String, String> unzippedMap = ZipHelperTest.unzipHelper(s3FileBytes);
        assertEquals(unzippedMap.size(), 2);
        assertEquals(unzippedMap.get("csv.csv"), "dummy csv content");
        assertEquals(unzippedMap.get("bulkdownload.zip"), "dummy bulk download content");

        // validate expiration time
        DateTime expirationTime = expirationTimeCaptor.getValue();
        assertEquals(expirationTime.getMillis(), MOCK_NOW.plusHours(URL_EXPIRATION_HOURS).getMillis());

        // validate mock file helper is clean
        assertTrue(inMemoryFileHelper.isEmpty());
    }

    private void setupPackager(Map<String, UploadSchema> synapseTableToSchema,
            Map<String, SynapseTaskResultContent> synapseTableToResult,
            Map<String, ExecutionException> synapseTableToException) {
        // spy "now" and replace it with MOCK_NOW
        packager = new SynapsePackager();

        // Set the current time to MOCK_NOW, so we can test pre-signed URL expiration date appropriately.
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW.getMillis());

        // branch coverage: noop synapse helper
        packager.setSynapseHelper(mock(SynapseHelper.class));

        // mock file helper
        inMemoryFileHelper = new InMemoryFileHelper();
        packager.setFileHelper(inMemoryFileHelper);

        // mock executor service to just call the callables directly
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(mockExecutorService.submit(any(SynapseDownloadFromTableTask.class))).then(invocation -> {
            // validate params
            SynapseDownloadFromTableTask task = invocation.getArgumentAt(0, SynapseDownloadFromTableTask.class);
            SynapseDownloadFromTableParameters params = task.getParameters();
            String synapseTableId = params.getSynapseTableId();
            File tmpDir = params.getTempDir();

            assertEquals(params.getHealthCode(), TEST_HEALTH_CODE);
            assertEquals(params.getStartDate().toString(), TEST_START_DATE);
            assertEquals(params.getEndDate().toString(), TEST_END_DATE);
            assertNotNull(tmpDir);
            assertSame(params.getSchema(), synapseTableToSchema.get(synapseTableId));

            Future<SynapseDownloadFromTableResult> mockFuture = mock(Future.class);

            // If we have an exception in the exception map, the future should throw that.
            if (synapseTableToException != null) {
                ExecutionException ex = synapseTableToException.get(synapseTableId);
                if (ex != null) {
                    when(mockFuture.get()).thenThrow(ex);
                    return mockFuture;
                }
            }

            // create a mock Future that returns the result from the synapseTableToResult map
            SynapseTaskResultContent taskResultContent = synapseTableToResult.get(synapseTableId);
            SynapseDownloadFromTableResult.Builder taskResultBuilder = new SynapseDownloadFromTableResult.Builder();
            if (taskResultContent.getCsvFileContent() != null) {
                File csvFile = createFileWithContent(tmpDir, taskResultContent.getCsvFileName(),
                        taskResultContent.getCsvFileContent());
                taskResultBuilder.withCsvFile(csvFile);
            }
            if (taskResultContent.getBulkDownloadFileContent() != null) {
                File bulkDownloadFile = createFileWithContent(tmpDir, taskResultContent.getBulkDownloadFileName(),
                        taskResultContent.getBulkDownloadFileContent());
                taskResultBuilder.withBulkDownloadFile(bulkDownloadFile);
            }

            when(mockFuture.get()).thenReturn(taskResultBuilder.build());
            return mockFuture;
        });
        packager.setAuxiliaryExecutorService(mockExecutorService);

        // Use real zip helper. It's easier to use the real one than to mock it out.
        ZipHelper zipHelper = new ZipHelper();
        zipHelper.setFileHelper(inMemoryFileHelper);
        packager.setZipHelper(zipHelper);

        // mock config
        Config mockConfig = mock(Config.class);
        when(mockConfig.getInt(SynapsePackager.CONFIG_KEY_EXPIRATION_HOURS)).thenReturn(URL_EXPIRATION_HOURS);
        when(mockConfig.get(SynapsePackager.CONFIG_KEY_USERDATA_BUCKET)).thenReturn(DUMMY_USER_DATA_BUCKET);
        packager.setConfig(mockConfig);

        // Clean up s3FileBytes. Apparently, TestNG doesn't clean state between each test.
        s3FileBytes = null;

        // mock S3 helper
        // Different tests do different things with pre-signed URL, so leave that one alone.
        mockS3Helper = mock(S3Helper.class);
        doAnswer(invocation -> {
            // on cleanup, the file is destroyed, so we need to intercept that file now
            File s3File = invocation.getArgumentAt(2, File.class);
            s3FileBytes = inMemoryFileHelper.getBytes(s3File);

            // needed because Answer declares a return type, even if it's Void
            return null;
        }).when(mockS3Helper).writeFileToS3(eq(DUMMY_USER_DATA_BUCKET), startsWith(TEST_MASTER_ZIP_FILE_PREFIX),
                any(File.class));
        packager.setS3Helper(mockS3Helper);
    }

    private File createFileWithContent(File tmpDir, String filename, String content) throws Exception {
        File file = inMemoryFileHelper.newFile(tmpDir, filename);
        try (Writer fileWriter = inMemoryFileHelper.getWriter(file)) {
            fileWriter.write(content);
        }
        return file;
    }

    // Because we can't create the files until the temp dir is created, and we can't create the temp dir until we
    // execute the test. So the setup method will take the task result file contents and inject them in both the mock
    // file system and into the actual result.
    static class SynapseTaskResultContent {
        private final String csvFileName;
        private final String csvFileContent;
        private final String bulkDownloadFileName;
        private final String bulkDownloadFileContent;

        SynapseTaskResultContent(String csvFileName, String csvFileContent, String bulkDownloadFileName,
                String bulkDownloadFileContent) {
            this.csvFileName = csvFileName;
            this.csvFileContent = csvFileContent;
            this.bulkDownloadFileName = bulkDownloadFileName;
            this.bulkDownloadFileContent = bulkDownloadFileContent;
        }

        String getCsvFileName() {
            return csvFileName;
        }

        String getCsvFileContent() {
            return csvFileContent;
        }

        String getBulkDownloadFileName() {
            return bulkDownloadFileName;
        }

        String getBulkDownloadFileContent() {
            return bulkDownloadFileContent;
        }
    }
}
