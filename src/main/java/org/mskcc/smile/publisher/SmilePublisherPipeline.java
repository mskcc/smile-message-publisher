package org.mskcc.smile.publisher;

import java.util.Date;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.smile.publisher.pipeline.config.BatchConfiguration;
import org.mskcc.smile.publisher.pipeline.limsrest.LimsRequestUtil;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication
public class SmilePublisherPipeline {

    private static final Log LOG = LogFactory.getLog(SmilePublisherPipeline.class);

    public static void main(String[] args) throws Exception {
        SpringApplication app = new SpringApplication(SmilePublisherPipeline.class);
        ConfigurableApplicationContext ctx = app.run(args);
        CommandLine commandLine = parseArgs(args);

        String jobName = null;
        JobParametersBuilder jobParamsBuilder = new JobParametersBuilder();
        if (commandLine.hasOption("m") && commandLine.hasOption("r")) {
            jobName = BatchConfiguration.SMILE_SERVICE_PUBLISHER_JOB;
            jobParamsBuilder.addString("requestIds", commandLine.getOptionValue("r"));
        } else if (commandLine.hasOption("r") || commandLine.hasOption("s")) {
            // validatate format for start date and end date (if applicable)
            if (commandLine.hasOption("s")) {
                validateProvidedDates(commandLine.getOptionValue("s"), commandLine.getOptionValue("e"));
            }
            // set up job params for lims request publisher job
            jobName = BatchConfiguration.LIMS_REQUEST_PUBLISHER_JOB;
            jobParamsBuilder.addString("requestIds", commandLine.getOptionValue("r"))
                .addString("startDate", commandLine.getOptionValue("s"))
                .addString("endDate", commandLine.getOptionValue("e"))
                .addString("cmoRequestsFilter", String.valueOf(commandLine.hasOption("c")));
        } else if (commandLine.hasOption("f")) {
            jobName = BatchConfiguration.FILE_PUBLISHER_JOB;
            jobParamsBuilder.addString("publisherFilename", commandLine.getOptionValue("f"));
        } else if (commandLine.hasOption("j")) {
            jobName = BatchConfiguration.JSON_FILE_PUBLISHER_JOB;
            jobParamsBuilder.addString("jsonFilename", commandLine.getOptionValue("j"))
                    .addString("publisherTopic", commandLine.getOptionValue("t"));
        }

        // set up job, job launcher, and job execution
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        Job job = ctx.getBean(jobName, Job.class);
        JobExecution jobExecution = jobLauncher.run(job, jobParamsBuilder.toJobParameters());
        if (!jobExecution.getExitStatus().equals(ExitStatus.COMPLETED)) {
            LOG.error(jobName + " failed with exit status: " + jobExecution.getExitStatus());
        } else {
            LOG.info("Job completed with exit status: '" + jobExecution.getExitStatus().getExitCode()
                    + "' - exiting application");
        }
        System.exit(SpringApplication.exit(ctx));
    }

    /**
     * Validate the start and end dates provided if applicable.
     * @param startDate
     * @param endDate
     */
    private static void validateProvidedDates(String startDate, String endDate) {
        Date startTimestamp = null;
        Date endTimestamp = null;
        // parse start date
        try {
            startTimestamp = LimsRequestUtil.DATE_FORMAT.parse(startDate);
        } catch (java.text.ParseException ex) {
            LOG.error("Error parsing start date - must be provided in format: YYYY/MM/DD");
            System.exit(2);
        }
        // parse end date if provided
        if (endDate != null) {
            try {
                endTimestamp = LimsRequestUtil.DATE_FORMAT.parse(endDate);
            } catch (java.text.ParseException ex) {
                LOG.error("Error parsing end date - must be provided in format: YYYY/MM/DD");
                System.exit(2);
            }
            // also check that end timestamp occurs after start timestamp
            if (endTimestamp.before(startTimestamp)) {
                LOG.error("End date provided must occur after the start date provided.");
                System.exit(2);
            }
        }
    }

    private static Options getOptions(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "shows this help document and quits.")
                .addOption("r", "request_ids", true, "Comma-separated list of request ids to fetch "
                + "data for from LimsRest [REQUEST IDS MODE]")
                .addOption("s", "start_date", true, "Start date [YYYY/MM/DD], fetch requests from "
                        + "LimsRest beginning from the given start date [START/END DATE MODE]")
                .addOption("e", "end_date", true, "End date [YYYY/MM/DD]. Fetch requests from LimsRest "
                        + "between the start and end dates provided. [OPTIONAL, START/END DATE MODE]")
                .addOption("c", "cmo_requests", false, "Filter LIMS requests by CMO requests only "
                        + "[OPTIONAL, START/END MODE & REQUEST IDS MODE]")
                .addOption("f", "publisher_filename", true, "Input publisher filename [FILE READING MODE]")
                .addOption("m", "smile_service_mode", false, "Runs in Smile Service mode")
                .addOption("j", "json_filename", true, "Publishes contents from provided JSON file. "
                        + "[JSON FILE READING MODE]")
                .addOption("t", "topic", true, "Topic to publish to when running in JSON FILE READING MODE");
        return options;
    }

    private static void help(Options options, int exitStatus) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("CmoNewRequestPublisher", options);
        System.exit(exitStatus);
    }

    private static CommandLine parseArgs(String[] args) throws Exception {
        Options options = getOptions(args);
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("h")
                || (!commandLine.hasOption("r") && !commandLine.hasOption("m")
                && !commandLine.hasOption("s") && !commandLine.hasOption("f")
                && !commandLine.hasOption("j"))) {
            help(options, 0);
        }
        // check that command line options entered are valid
        if (commandLine.hasOption("r") && (commandLine.hasOption("s")
                || commandLine.hasOption("e"))) {
            LOG.error("Cannot use '--request_ids with '--start_date' or '--end_date'");
            help(options, 1);
        } else if (commandLine.hasOption("f") && (commandLine.hasOption("r")
                || (commandLine.hasOption("s") || commandLine.hasOption("e"))
                || commandLine.hasOption("m"))) {
            LOG.error("Cannot use '--publisher_filename' with '--request_ids' or"
                    + "'--start_date | --end_date' or '--smile_service_mode'");
            help(options, 1);
        } else if (commandLine.hasOption("c") && (commandLine.hasOption("f")
                || commandLine.hasOption("m"))) {
            LOG.error("Cannot use --cmo_requests option with --publisher_filename or --smile_service_mode "
                    + "or --json_filename");
            help(options, 1);
        } else if (commandLine.hasOption("m") && !commandLine.hasOption("r")) {
            LOG.error("Must run '-m' option with '-r'");
            help(options, 1);
        } else if (commandLine.hasOption("j") && !commandLine.hasOption("t")) {
            LOG.error("Must run --json_filename option with --topic");
            help(options, 1);
        } else if (!commandLine.hasOption("h") && !commandLine.hasOption("s")
                && !commandLine.hasOption("f") && !commandLine.hasOption("r")
                && !commandLine.hasOption("m") && !commandLine.hasOption("j")) {
            LOG.error("Must run application with at least option '-r', '-s', '-m', '-f', "
                    + "or '-j' - exiting...");
            help(options, 1);
        }
        return commandLine;
    }

}
