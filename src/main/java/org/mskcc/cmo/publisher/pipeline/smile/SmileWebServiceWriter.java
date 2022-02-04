package org.mskcc.cmo.publisher.pipeline.smile;

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
public class SmileWebServiceWriter implements ItemStreamWriter<String> {

    @Autowired
    private Gateway messagingGateway;

    @Value("${smile.cmo_new_request_topic}")
    private String SMILE_NEW_REQUEST_TOPIC;

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(SmileWebServiceWriter.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public void write(List<? extends String> requestResponseList) throws Exception {
        for (String requestJson : requestResponseList) {
            Map<String, Object> reqMap = mapper.readValue(requestJson, Map.class);
            String requestId = (String) reqMap.get("requestId");
            try {
                messagingGateway.publish(requestId, SMILE_NEW_REQUEST_TOPIC, requestJson);
            } catch (Exception e) {
                LOG.error("Error during attempt to publish on topic '" + SMILE_NEW_REQUEST_TOPIC
                        + "' for request: " + requestId, e);
            }
        }
    }
}
