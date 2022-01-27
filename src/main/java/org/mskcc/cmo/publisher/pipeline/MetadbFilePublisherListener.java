package org.mskcc.cmo.publisher.pipeline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

/**
 *
 * @author ochoaa
 */
public class MetadbFilePublisherListener implements StepExecutionListener {
    private static final Log LOG = LogFactory.getLog(MetadbFilePublisherListener.class);

    @Override
    public void beforeStep(StepExecution se) {}

    @Override
    public ExitStatus afterStep(StepExecution se) {
        LOG.info("Finished publishing messages from input file...returning exit"
                + " status 'COMPLETED'");
        return ExitStatus.COMPLETED;
    }

}
