package org.sagebionetworks.bridge.udd.synapse;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.testng.annotations.Test;

import org.sagebionetworks.bridge.udd.dynamodb.UploadSchema;
import org.sagebionetworks.bridge.udd.helper.MockFileHelper;

@SuppressWarnings("unchecked")
public class SynapsePackagerTest {
    @Test
    public void noFiles() {
        // TODO
    }

    // error in pre-signed URL
    // normal case
    // * task with no files
    // * task with CSV
    // * task with CSV and bulk download
    // * task with errors

    private static void setupTestWithArgs(Map<String, UploadSchema> synapseTableToSchema,
            Map<String, SynapseDownloadFromTableResult> synapseTableToResult) {
        SynapsePackager packager = new SynapsePackager();

        // mock file helper
        MockFileHelper mockFileHelper = new MockFileHelper();
        packager.setFileHelper(mockFileHelper);

        // mock executor service to just call the callables directly
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        when(mockExecutorService.submit(any(SynapseDownloadFromTableTask.class))).then(invocation -> {
            // create a mock Future that returns the result from the synapseTableToResult map
            SynapseDownloadFromTableTask task = invocation.getArgumentAt(0, SynapseDownloadFromTableTask.class);

            // TODO validate params

            SynapseDownloadFromTableResult taskResult = synapseTableToResult.get(task.getParameters()
                    .getSynapseTableId());

            Future<SynapseDownloadFromTableResult> mockFuture = mock(Future.class);
            when(mockFuture.get()).thenReturn(taskResult);
            return mockFuture;
        });
    }

    // TODO

    // clean up no master zip
    // clean up master zip doesn't exist
    // clean up files and master zip
    // branch coverage: some files don't exist
}
