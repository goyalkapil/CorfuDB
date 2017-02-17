package org.corfudb.runtime.view;

import com.google.common.io.Files;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DuplicateLogView Tests.
 * <p>
 * Created by zlokhandwala on 2/16/17.
 */
public class DuplicateLogViewTest extends AbstractViewTest {

    /**
     * Scenario:
     * One Corfu Server PORT_0 started in non-memory mode with
     * log directory as the TEST_TEMP_DIR.
     * We then run the logDuplication to duplicate all files in
     * a local test directory.
     * Finally we assert the contents of both sets of files.
     *
     * @throws Exception
     */
    @Test
    public void logDuplication()
            throws Exception {

        // Number of 10,000 entries.
        final int entryFileCount = 5;

        addServer(SERVERS.PORT_0,
                new ServerContextBuilder()
                        .setSingle(false)
                        .setMemory(false)
                        .setLogPath(PARAMETERS.TEST_TEMP_DIR)
                        .setServerRouter(new TestServerRouter(SERVERS.PORT_0))
                        .setPort(SERVERS.PORT_0)
                        .build()
        );

        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);

        CorfuRuntime corfuRuntime = new CorfuRuntime();
        l.getLayoutServers().forEach(corfuRuntime::addLayoutServer);
        corfuRuntime.connect();

        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = ("hello world. New Junk data.").getBytes();

        IStreamView sv = corfuRuntime.getStreamsView().get(streamA);
        // Creates 50,000 entries. Resulting in 5 log files.
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE * entryFileCount; i++) {
            sv.append(testPayload);
        }

        // Create new test directory for log files.
        new File(PARAMETERS.TEST_TEMP_DIR + File.separator + "test").mkdir();
        String logPath = PARAMETERS.TEST_TEMP_DIR + File.separator + "log";
        String testPath = PARAMETERS.TEST_TEMP_DIR + File.separator + "test";

        // Execute log duplication.
        DuplicateLogView duplicateLogView = new DuplicateLogView(corfuRuntime);
        duplicateLogView.duplicateLogFromSource(SERVERS.ENDPOINT_0, testPath);

        // Assert if all files copied successfully.
        File[] originalFiles = new File(logPath).listFiles();
        File[] copiedFiles = new File(testPath).listFiles();
        assertThat(originalFiles.length).isEqualTo(copiedFiles.length).isEqualTo(entryFileCount);
        for (int i = 0; i < originalFiles.length; i++) {
            assertThat(Files.equal(originalFiles[i], copiedFiles[i])).isTrue();
        }
    }
}
