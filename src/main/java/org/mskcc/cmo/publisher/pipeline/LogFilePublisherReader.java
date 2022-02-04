package org.mskcc.cmo.publisher.pipeline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Value;

/**
 *
 * @author ochoaa
 */
public class LogFilePublisherReader implements ItemStreamReader<Map<String, String>> {
    @Value("#{jobParameters[publisherFilename]}")
    private String publisherFilename;

    private List<Map<String, String>> messagesToPublish;

    private static final Log LOG = LogFactory.getLog(LogFilePublisherReader.class);

    @Override
    public void open(ExecutionContext ec) throws ItemStreamException {
        // read the file, make sure it exists
        File publisherFile = new File(publisherFilename);
        if (!publisherFile.exists()) {
            throw new RuntimeException("File does not exist: " + publisherFilename);
        }
        // load records from input file
        try {
            this.messagesToPublish = loadPublisherMessageRecords(publisherFile);
        } catch (IOException ex) {
            throw new ItemStreamException("Encountered error while loading data from input publisher file",
                    ex);
        }
    }

    /**
     * Loads records from input publisher file.
     * @param publisherFile
     * @return List
     * @throws FileNotFoundException
     * @throws IOException
     */
    private List<Map<String, String>> loadPublisherMessageRecords(File publisherFile)
            throws FileNotFoundException, IOException {
        List<Map<String, String>> filedata = new ArrayList<>();

        FileReader reader = new FileReader(publisherFile);
        BufferedReader buff = new BufferedReader(reader);
        String line = buff.readLine();
        int lineCount = 1;

        // keep reading until end of file
        while (line != null) {
            String[] parts = line.split("\t");
            // simple sanity checking, make sure there are 3 elements in split line
            if (parts.length != 3) {
                LOG.error("Line number " + String.valueOf(lineCount) + " is not expected size (3),"
                        + "actual size: " + String.valueOf(parts.length)
                        + "\n\tline: " + line);
                throw new RuntimeException("Exception during reading of file: " + publisherFilename);
            }
            // publisher topic is in column 2, message is column 3
            Map<String, String> record = new HashMap<>();
            record.put("topic", parts[1]);
            record.put("message", parts[2]);
            filedata.add(record);
            lineCount++;
            line = buff.readLine();
        }
        reader.close();

        LOG.info("Loaded " + filedata.size() + " records from input file: " + publisherFilename);
        return filedata;
    }

    @Override
    public void update(ExecutionContext ec) throws ItemStreamException {}

    @Override
    public void close() throws ItemStreamException {}

    @Override
    public Map<String, String> read() throws Exception, UnexpectedInputException,
            ParseException, NonTransientResourceException {
        if (!messagesToPublish.isEmpty()) {
            return messagesToPublish.remove(0);
        }
        return null;
    }

}
