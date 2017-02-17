package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request for a file chunk.
 * This request is sent to a log unit server by a client.
 * It contains the name of the file which is required,
 * the offset and the size of the chunk required.
 * <p>
 * Created by zlokhandwala on 2/15/17.
 */
@Data
@AllArgsConstructor
public class ChunkedFileRequest implements ICorfuPayload<ChunkedFileRequest> {

    String fileName;
    long offset;
    int chunkSize;

    public ChunkedFileRequest(ByteBuf buf) {
        fileName = ICorfuPayload.fromBuffer(buf, String.class);
        offset = ICorfuPayload.fromBuffer(buf, Long.class);
        chunkSize = ICorfuPayload.fromBuffer(buf, Integer.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, fileName);
        ICorfuPayload.serialize(buf, offset);
        ICorfuPayload.serialize(buf, chunkSize);
    }
}
