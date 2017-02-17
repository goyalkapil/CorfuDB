package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.BulkReadInitResponse;
import org.corfudb.protocols.wireprotocol.ChunkedFileResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.FileModificationException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Duplicate Log View.
 * To copy the log files from the source log unit server to the
 * target directory.
 * <p>
 * Created by zlokhandwala on 2/16/17.
 */
@Slf4j
public class DuplicateLogView extends AbstractView {

    /**
     * Number of bytes requested in every chunk.
     */
    private final int chunkSize = 1_000_000;

    public DuplicateLogView(CorfuRuntime runtime) {
        super(runtime);
    }

    public boolean duplicateLogFromSource(String source, String targetDirPath) {

        // Clear the target directory.
        log.warn("Existing files in target directory will be cleared");
        File targetDir = new File(targetDirPath);
        Arrays.stream(targetDir.listFiles()).forEach(File::delete);

        try {
            // Initiate the bulk transfer. Get the initiation response which
            // contains the list of file names to be copied.
            BulkReadInitResponse bulkReadInitResponse = runtime.getRouter(source).getClient(LogUnitClient.class)
                    .initiateBulkFileRead().get();

            List<String> fileNames = bulkReadInitResponse.getFileNames();

            for (String fileName : fileNames) {
                File targetFile = new File(targetDir + File.separator + fileName);
                FileOutputStream fileOutputStream = new FileOutputStream(targetFile);
                long offset = 0;
                long fileLastModified = 0;

                // For every file to be copied request chunks of file.
                while (true) {
                    ChunkedFileResponse chunkedFileResponse = runtime.getRouter(source).getClient(LogUnitClient.class)
                            .readFileChunk(fileName, offset, chunkSize).get();

                    // Check if file not modfied between chunk requests. If yes abort.
                    // Can we do better ?
                    if (fileLastModified == 0) {
                        fileLastModified = chunkedFileResponse.getLastModified();
                    } else if (fileLastModified != chunkedFileResponse.getLastModified()) {
                        throw new FileModificationException("File modified while copying");
                    }

                    // Compare the checksum to ensure data integrity.
                    // If Checksum does not match retry requesting the last chunk.
                    fileOutputStream.write(chunkedFileResponse.getFileChunk());
                    Checksum checksum = new CRC32();
                    checksum.update(chunkedFileResponse.getFileChunk(), 0, chunkedFileResponse.getFileChunk().length);
                    if (checksum.getValue() != chunkedFileResponse.getChecksum()) {
                        log.warn("Checksum does not match. Retrying previous chunk.");
                        continue;
                    }

                    // Break if no more bytes left to be copied.
                    if (chunkedFileResponse.getRemaining() == 0) break;
                    offset = chunkedFileResponse.getNextOffset();
                }
                fileOutputStream.close();
            }
        } catch (InterruptedException ie) {
            log.error("Interrupted Exception : {}", ie);
            return false;
        } catch (ExecutionException ee) {
            log.error("Execution Exception : {}", ee);
            return false;
        } catch (IOException ioe) {
            log.error("IO Exception : {}", ioe);
            return false;
        } catch (FileModificationException fme) {
            log.error("File Modification Exception : {}", fme);
            return false;
        }

        return false;
    }

}
