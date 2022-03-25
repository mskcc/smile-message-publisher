package org.mskcc.smile.publisher.pipeline.limsrest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class LimsRequestWriter implements ItemStreamWriter<Map<String, Object>> {

    @Autowired
    private Gateway messagingGateway;

    @Value("${lims.publisher_topic}")
    private String LIMS_PUBLISHER_TOPIC;

    private ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(LimsRequestWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends Map<String, Object>> requestResponseList) throws Exception {
        for (Map<String, Object> request : requestResponseList) {
            String requestId = (String) request.get("requestId");
            String requestJson = mapper.writeValueAsString(request);
            LOG.debug("\nPublishing IGO new request to SMILE:\n\n"
                    + requestJson + "\n\n on topic: " + LIMS_PUBLISHER_TOPIC);
            try {
                messagingGateway.publish(LIMS_PUBLISHER_TOPIC, requestJson);
            } catch (Exception e) {
                LOG.error("Error during attempt to publish on topic '" + LIMS_PUBLISHER_TOPIC
                        + "' for request: " + requestId, e);
            }
        }
    }

}
