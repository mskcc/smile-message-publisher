package org.mskcc.smile.publisher.pipeline.limsrest;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class LimsRequestReader implements ItemStreamReader<String> {

    @Value("#{jobParameters[requestIds]}")
    private String requestIds;

    @Value("#{jobParameters[startDate]}")
    private String startDate;

    @Value("#{jobParameters[endDate]}")
    private String endDate;

    @Autowired
    private LimsRequestUtil limsRestUtil;

    private List<String> requestIdsList;

    private static final Log LOG = LogFactory.getLog(LimsRequestReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        if (requestIds == null || requestIds.isEmpty()) {
            LOG.info("Fetching data from LimsRest by the provided timestamp(s)....");
            try {
                this.requestIdsList = limsRestUtil.getRequestIdsByDate(startDate,
                        endDate);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } else {
            this.requestIdsList = new LinkedList<>(Arrays.asList(requestIds.split(",")));
        }
        LOG.info("Fetching sample manifests for " + requestIdsList.size() + " requests...");
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException,
            NonTransientResourceException {
        if (!requestIdsList.isEmpty()) {
            return requestIdsList.remove(0);
        }
        return null;
    }

}
