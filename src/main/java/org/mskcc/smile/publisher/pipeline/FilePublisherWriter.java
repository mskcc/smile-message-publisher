package org.mskcc.smile.publisher.pipeline;

import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author ochoaa
 */
public class FilePublisherWriter implements ItemStreamWriter<Map<String, String>> {
    @Autowired
    private Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(FilePublisherWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Map<String, String>> messagesToPublish) throws Exception {
        for (Map<String, String> record : messagesToPublish) {
            LOG.debug("Publishing message: " + record.get("topic") + ", " + record.get("message"));
            messagingGateway.publish(record.get("topic"), record.get("message"));
        }
    }

}
