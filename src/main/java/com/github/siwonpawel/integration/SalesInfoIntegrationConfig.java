package com.github.siwonpawel.integration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.DefaultFileNameGenerator;
import org.springframework.integration.file.FileNameGenerator;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class SalesInfoIntegrationConfig {

    @Value("${sales.info.directory}")
    private String salesDirectory;

    @Bean
    public IntegrationFlow integrationFlow(JobRepository jobRepository, TaskExecutor integrationFlowTaskExecutor, Job importSalesInfo) {
        return IntegrationFlows
                .from(fileReadingMessageSource(),
                        sourcePolling -> sourcePolling.poller(Pollers.fixedDelay(Duration.ofSeconds(5)).maxMessagesPerPoll(1)))
                .channel(fileIn())
                .handle(fileRenameProcessingHandler(processingFilenameGenerator()))
                .transform(fileMessageToJobRequest(importSalesInfo))
                .handle(jobLaunchingGateway(jobRepository, integrationFlowTaskExecutor))
                .log()
                .get();

    }

    @Bean
    TaskExecutor integrationFlowTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        threadPoolTaskExecutor.setCorePoolSize(5);
        threadPoolTaskExecutor.setMaxPoolSize(5);
        threadPoolTaskExecutor.setQueueCapacity(10);
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.setThreadNamePrefix("Thread IF -> :");

        return threadPoolTaskExecutor;
    }

    FileReadingMessageSource fileReadingMessageSource() {
        FileReadingMessageSource messageSource = new FileReadingMessageSource();
        messageSource.setDirectory(new File(salesDirectory));
        messageSource.setFilter(new SimplePatternFileListFilter("*.csv"));

        return messageSource;
    }

    DirectChannel fileIn() {
        return new DirectChannel();
    }

    FileWritingMessageHandler fileRenameProcessingHandler(FileNameGenerator processingFilenameGenerator) {
        var fileWritingMessage = new FileWritingMessageHandler(new File(salesDirectory));

        fileWritingMessage.setDeleteSourceFiles(true);
        fileWritingMessage.setFileExistsMode(FileExistsMode.REPLACE);
        fileWritingMessage.setFileNameGenerator(processingFilenameGenerator);
        fileWritingMessage.setRequiresReply(false);

        return fileWritingMessage;
    }

    FileNameGenerator processingFilenameGenerator() {
        var filenameGenerator = new DefaultFileNameGenerator();
        filenameGenerator.setExpression("payload.name + '.processing'");

        return filenameGenerator;
    }

    FileMessageToJobRequest fileMessageToJobRequest(Job importSalesInfo) {
        var transformer = new FileMessageToJobRequest();
        transformer.setJob(importSalesInfo);

        return transformer;
    }

    JobLaunchingGateway jobLaunchingGateway(JobRepository jobRepository, TaskExecutor taskExecutor) {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);
        simpleJobLauncher.setTaskExecutor(taskExecutor);

        return new JobLaunchingGateway(simpleJobLauncher);
    }
}
