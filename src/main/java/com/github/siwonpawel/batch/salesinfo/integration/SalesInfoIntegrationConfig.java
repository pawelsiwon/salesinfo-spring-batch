package com.github.siwonpawel.batch.salesinfo.integration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SyncTaskExecutor;
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

import java.io.File;
import java.time.Duration;

@Configuration
public class SalesInfoIntegrationConfig {

    @Value("${sales.info.directory}")
    private String salesDirectory;

    @Bean
    public IntegrationFlow integrationFlow(JobRepository jobRepository, Job importSalesInfo, BeanFactory beanFactory) {
        return IntegrationFlows
                .from(fileReadingMessageSource(),
                        sourcePolling -> sourcePolling.poller(Pollers.fixedDelay(Duration.ofSeconds(5)).maxMessagesPerPoll(1)))
                .channel(fileIn())
                .handle(fileRenameProcessingHandler(processingFilenameGenerator(beanFactory)))
                .transform(fileMessageToJobRequest(importSalesInfo))
                .handle(jobLaunchingGateway(jobRepository, new SyncTaskExecutor()))
                .log()
                .get();

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

    FileNameGenerator processingFilenameGenerator(BeanFactory beanFactory) {
        var filenameGenerator = new DefaultFileNameGenerator();

        filenameGenerator.setBeanFactory(beanFactory);
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
