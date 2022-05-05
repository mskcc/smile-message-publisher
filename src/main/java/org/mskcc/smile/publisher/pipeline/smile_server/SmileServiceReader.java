package org.mskcc.smile.publisher.pipeline.smile_server;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class SmileServiceReader implements ItemStreamReader<String> {
    @Value("#{jobParameters[requestIds]}")
    private String requestIds;

    @Value("#{jobParameters[cmoRequestsFilter] ?: true}")
    private Boolean cmoRequestsFilter;

    @Autowired
    private SmileServiceUtil smileServiceUtil;

    private List<String> smileRequestsList;

    private final ObjectMapper mapper = new ObjectMapper();
    private static final Log LOG = LogFactory.getLog(SmileServiceReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        List<String> toReturn = new ArrayList<>();
        for (String requestId : Arrays.asList(requestIds.split(","))) {
            try {
                String requestJson = smileServiceUtil.getRequestById(requestId);
                Map<String, Object> requestMap = mapper.readValue(requestJson, Map.class);
                Boolean isCmoRequest = (Boolean) requestMap.getOrDefault("isCmoRequest", Boolean.FALSE);
                // check if cmo request filter is enabled
                if (cmoRequestsFilter && !isCmoRequest) {
                    LOG.info("Skipping non-CMO request: " + requestId + "...");
                    continue;
                }
                toReturn.add(requestJson);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        LOG.info("Total requests publishing to topic: " + String.valueOf(toReturn.size()));
        this.smileRequestsList = toReturn;
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public String read() throws Exception {
        if (!smileRequestsList.isEmpty()) {
            return smileRequestsList.remove(0);
        }
        return null;
    }

}
