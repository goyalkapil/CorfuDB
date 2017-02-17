package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Response containing the file chunk requested.
 * The file chunk request is responded by the following:
 * Name of the file,
 * Next offset which needs to be requested,
 * Bytes remaining in the file,
 * File Chunk
 * Checksum of the file chunk.
 * <p>
 * Created by zlokhandwala on 2/15/17.
 */
@Data
@AllArgsConstructor
public class ChunkedFileResponse implements ICorfuPayload<ChunkedFileResponse> {

    String fileName;
    long nextOffset;
    long remaining;
    byte[] fileChunk;
    long checksum;
    long lastModified;

    public ChunkedFileResponse(ByteBuf buf) {
        fileName = ICorfuPayload.fromBuffer(buf, String.class);
        nextOffset = ICorfuPayload.fromBuffer(buf, Long.class);
        remaining = ICorfuPayload.fromBuffer(buf, Long.class);
        fileChunk = ICorfuPayload.fromBuffer(buf, byte[].class);
        checksum = ICorfuPayload.fromBuffer(buf, Long.class);
        lastModified = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, fileName);
        ICorfuPayload.serialize(buf, nextOffset);
        ICorfuPayload.serialize(buf, remaining);
        ICorfuPayload.serialize(buf, fileChunk);
        ICorfuPayload.serialize(buf, checksum);
        ICorfuPayload.serialize(buf, lastModified);
    }
}
