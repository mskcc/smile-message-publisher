package org.mskcc.smile.publisher.pipeline.config;

import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.smile.publisher.pipeline.FilePublisherListener;
import org.mskcc.smile.publisher.pipeline.FilePublisherReader;
import org.mskcc.smile.publisher.pipeline.FilePublisherWriter;
import org.mskcc.smile.publisher.pipeline.JsonFileTasklet;
import org.mskcc.smile.publisher.pipeline.limsrest.LimsRequestListener;
import org.mskcc.smile.publisher.pipeline.limsrest.LimsRequestProcessor;
import org.mskcc.smile.publisher.pipeline.limsrest.LimsRequestReader;
import org.mskcc.smile.publisher.pipeline.limsrest.LimsRequestWriter;
import org.mskcc.smile.publisher.pipeline.smile_server.SmileServiceReader;
import org.mskcc.smile.publisher.pipeline.smile_server.SmileServiceWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 *
 * @author ochoaa
 */
@Configuration
@EnableBatchProcessing
@EnableAsync
@ComponentScan(basePackages = {"org.mskcc.cmo.messaging", "org.mskcc.cmo.common.*"})
public class BatchConfiguration {

    public static final String LIMS_REQUEST_PUBLISHER_JOB = "limsRequestPublisherJob";
    public static final String FILE_PUBLISHER_JOB = "filePublisherJob";
    public static final String SMILE_SERVICE_PUBLISHER_JOB = "smileServicePublisherJob";
    public static final String JSON_FILE_PUBLISHER_JOB = "jsonFilePublisherJob";

    @Value("${chunk.interval:10}")
    private Integer chunkInterval;

    @Value("${async.thread_pool_size:5}")
    private Integer asyncThreadPoolSize;

    @Value("${async.thread_pool_max:10}")
    private Integer asyncThreadPoolMax;

    @Value("${processor.thread_pool_size:5}")
    private Integer processorThreadPoolSize;

    @Value("${processor.thread_pool_max:10}")
    private Integer processorThreadPoolMax;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private Gateway messagingGateway;

    @Bean
    public Gateway messagingGateway() throws Exception {
        messagingGateway.connect();
        return messagingGateway;
    }

    /**
     * limsRequestPublisherJob
     * @return
     */
    @Bean
    public Job limsRequestPublisherJob() {
        return jobBuilderFactory.get(LIMS_REQUEST_PUBLISHER_JOB)
                .start(limsRequestPublisherStep())
                .build();
    }

    /**
     * filePublisherJob
     * @return
     */
    @Bean
    public Job filePublisherJob() {
        return jobBuilderFactory.get(FILE_PUBLISHER_JOB)
                .start(filePublisherStep())
                .build();
    }

    /**
     * Json file reading job.
     * @return
     */
    @Bean
    public Job jsonFilePublisherJob() {
        return jobBuilderFactory.get(JSON_FILE_PUBLISHER_JOB)
                .start(jsonFileStep())
                .build();
    }

    /**
     * limsRequestPublisherStep
     * @return
     */
    @Bean
    public Step limsRequestPublisherStep() {
        return stepBuilderFactory.get("limsRequestPublisherStep")
                .listener(limsRequestListener())
                .<String, Future<Map<String,Object>>>chunk(chunkInterval)
                .reader(limsRequestReader())
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .build();
    }

    /**
     * filePublisherStep
     * @return
     */
    @Bean
    public Step filePublisherStep() {
        return stepBuilderFactory.get("filePublisherStep")
                .listener(filePublisherListener())
                .<Map<String, String>, Map<String, String>>chunk(10)
                .reader(filePublisherReader())
                .writer(filePublisherWriter())
                .build();
    }

    /**
     * smileServicePublisherJob
     * @return
     */
    @Bean
    public Job smileServicePublisherJob() {
        return jobBuilderFactory.get(SMILE_SERVICE_PUBLISHER_JOB)
                .start(smileServicePublisherStep())
                .build();
    }

    /**
     * smileServicePublisherStep
     * @return
     */
    @Bean
    public Step smileServicePublisherStep() {
        return stepBuilderFactory.get("smileServicePublisherStep")
                .<String, String>chunk(10)
                .reader(mdbServiceReader())
                .writer(mdbServiceWriter())
                .build();
    }

    /**
     * Json file reading step.
     * @return
     */
    @Bean
    public Step jsonFileStep() {
        return stepBuilderFactory.get("jsonFileStep")
                .tasklet(jsonFileTasklet())
                .build();
    }

    /**
     * Json file reading and publisher tasklet.
     * @return
     */
    @Bean
    @StepScope
    public Tasklet jsonFileTasklet() {
        return new JsonFileTasklet();
    }

    /**
     * filePublisherReader
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamReader<Map<String, String>> filePublisherReader() {
        return new FilePublisherReader();
    }

    /**
     * filePublisherWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamWriter<Map<String, String>> filePublisherWriter() {
        return new FilePublisherWriter();
    }

    /**
     * filePublisherListener
     * @return
     */
    @Bean
    public StepExecutionListener filePublisherListener() {
        return new FilePublisherListener();
    }

    /**
     * asyncLimsRequestThreadPoolTaskExecutor
     * @return
     */
    @Bean(name = "asyncLimsRequestThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor asyncLimsRequestThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(asyncThreadPoolSize);
        executor.setMaxPoolSize(asyncThreadPoolMax);
        executor.initialize();
        return executor;
    }

    /**
     * processorThreadPoolTaskExecutor
     * @return
     */
    @Bean(name = "processorThreadPoolTaskExecutor")
    @StepScope
    public ThreadPoolTaskExecutor processorThreadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(processorThreadPoolSize);
        threadPoolTaskExecutor.setMaxPoolSize(processorThreadPoolMax);
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    /**
     * asyncItemProcessor
     * @return
     */
    @Bean
    @StepScope
    public ItemProcessor<String, Future<Map<String, Object>>> asyncItemProcessor() {
        AsyncItemProcessor<String, Map<String, Object>> asyncItemProcessor = new AsyncItemProcessor();
        asyncItemProcessor.setTaskExecutor(processorThreadPoolTaskExecutor());
        asyncItemProcessor.setDelegate(limsRequestProcessor());
        return asyncItemProcessor;
    }

    /**
     * limsRequestProcessor
     * @return
     */
    @Bean
    @StepScope
    public LimsRequestProcessor limsRequestProcessor() {
        return new LimsRequestProcessor();
    }

    /**
     * asyncItemWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemWriter<Future<Map<String, Object>>> asyncItemWriter() {
        AsyncItemWriter<Map<String, Object>> asyncItemWriter = new AsyncItemWriter();
        asyncItemWriter.setDelegate(limsRequestWriter());
        return asyncItemWriter;
    }

    /**
     * limsRequestWriter
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamWriter<Map<String, Object>> limsRequestWriter() {
        return new LimsRequestWriter();
    }

    /**
     * limsRequestReader
     * @return
     */
    @Bean
    @StepScope
    public ItemStreamReader<String> limsRequestReader() {
        return new LimsRequestReader();
    }

    /**
     * limsRequestListener
     * @return
     */
    @Bean
    public StepExecutionListener limsRequestListener() {
        return new LimsRequestListener();
    }

    @Bean
    @StepScope
    public ItemStreamWriter<String> mdbServiceWriter() {
        return new SmileServiceWriter();
    }

    @Bean
    @StepScope
    public ItemStreamReader<String> mdbServiceReader() {
        return new SmileServiceReader();
    }

    // general spring batch configuration
    @Value("org/springframework/batch/core/schema-drop-sqlite.sql")
    private Resource dropRepositoryTables;

    @Value("org/springframework/batch/core/schema-sqlite.sql")
    private Resource dataRepositorySchema;

    /**
     * Spring Batch datasource.
     * @return DataSource
     */
    @Bean
    public DataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.sqlite.JDBC");
        dataSource.setUrl("jdbc:sqlite:repository.sqlite");
        return dataSource;
    }

    /**
     * Spring Batch datasource initializer.
     * @param dataSource
     * @return DataSourceInitializer
     * @throws MalformedURLException
     */
    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource) throws MalformedURLException {
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();
        databasePopulator.addScript(dropRepositoryTables);
        databasePopulator.addScript(dataRepositorySchema);
        databasePopulator.setIgnoreFailedDrops(true);

        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);
        return initializer;
    }

    /**
     * Spring Batch job repository.
     * @return JobRepository
     * @throws Exception
     */
    private JobRepository getJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource());
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return (JobRepository) factory.getObject();
    }

    /**
     * Spring Batch transaction manager.
     * @return PlatformTransactionManager
     */
    private PlatformTransactionManager getTransactionManager() {
        return new ResourcelessTransactionManager();
    }

    /**
     * Spring Batch job launcher.
     * @return JobLauncher
     * @throws Exception
     */
    public JobLauncher getJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(getJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}
