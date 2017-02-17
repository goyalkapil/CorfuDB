package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Initializes the bulk read process.
 * The Server returns the list of files which it possesses at the time of request.
 * The client then drives the duplication process by querying every file.
 * <p>
 * Created by zlokhandwala on 2/15/17.
 */
@Data
@AllArgsConstructor
public class BulkReadInitResponse implements ICorfuPayload<BulkReadInitResponse> {

    List<String> fileNames;

    public BulkReadInitResponse(ByteBuf buf) {
        fileNames = ICorfuPayload.listFromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, fileNames);
    }
}
