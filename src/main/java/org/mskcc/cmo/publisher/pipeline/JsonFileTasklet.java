package org.mskcc.cmo.publisher.pipeline;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class JsonFileTasklet implements Tasklet {

    @Value("#{jobParameters[jsonFilename]}")
    private String jsonFilename;

    @Value("#{jobParameters[publisherTopic]}")
    private String publisherTopic;

    @Autowired
    private Gateway messagingGateway;

    private static final Log LOG = LogFactory.getLog(JsonFileTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution sc, ChunkContext cc) throws Exception {
        File jsonFile = new File(jsonFilename);
        if (!jsonFile.exists()) {
            throw new RuntimeException("File does not exist: " + jsonFilename);
        }
        String filedata = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
        if (filedata == null || filedata.isEmpty()) {
            throw new RuntimeException("Error reading filecontents from json file: " + jsonFilename);
        }
        messagingGateway.publish(publisherTopic, filedata);
        return RepeatStatus.FINISHED;
    }
}
