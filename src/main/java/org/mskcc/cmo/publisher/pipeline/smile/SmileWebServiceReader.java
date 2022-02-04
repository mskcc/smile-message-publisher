package org.mskcc.cmo.publisher.pipeline.smile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
public class SmileWebServiceReader implements ItemStreamReader<String> {
    @Value("#{jobParameters[requestIds]}")
    private String requestIds;

    @Autowired
    private SmileWebServiceUtil smileWebServiceUtil;

    private List<String> smileRequestsList;

    private static final Log LOG = LogFactory.getLog(SmileWebServiceReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        List<String> toReturn = new ArrayList<>();
        for (String requestId : Arrays.asList(requestIds.split(","))) {
            try {
                String requestJson = smileWebServiceUtil.getRequestById(requestId);
                toReturn.add(requestJson);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
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
