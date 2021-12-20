package org.mskcc.cmo.publisher.pipeline.metadb;

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
public class MetadbServiceReader implements ItemStreamReader<String> {
    @Value("#{jobParameters[requestIds]}")
    private String requestIds;

    @Autowired
    private MetadbServiceUtil metadbServiceUtil;

    private List<String> metadbRequestsList;

    private static final Log LOG = LogFactory.getLog(MetadbServiceReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        List<String> toReturn = new ArrayList<>();
        for (String requestId : Arrays.asList(requestIds.split(","))) {
            try {
                String requestJson = metadbServiceUtil.getRequestById(requestId);
                toReturn.add(requestJson);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        this.metadbRequestsList = toReturn;
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public String read() throws Exception {
        if (!metadbRequestsList.isEmpty()) {
            return metadbRequestsList.remove(0);
        }
        return null;
    }

}
