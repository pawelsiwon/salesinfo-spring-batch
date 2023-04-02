package com.github.siwonpawel.batch.salesinfo;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.batch.salesinfo.faulttolerance.CustomSkipPolicy;
import com.github.siwonpawel.batch.salesinfo.listeners.CustomStepExecutionListener;
import com.github.siwonpawel.batch.salesinfo.processor.SalesInfoProcessor;
import com.github.siwonpawel.domain.SalesInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManagerFactory;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SalesInfoJobConfig {

    private final JobRepository jobRepository;
    private final EntityManagerFactory entityMangerFactory;
    private final PlatformTransactionManager transactionManager;

    @Bean
    Job importSalesInfo(Step fromFileIntoDatabase) {
        return new JobBuilder("importSalesInfo")
                .repository(jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(fromFileIntoDatabase)
                .build();
    }

    @Bean
    Step fromFileIntoDatabase(
            ItemReader<SalesInfoDTO> salesInfoReader,
            AsyncItemProcessor<SalesInfoDTO, SalesInfo> asyncItemProcessor,
            AsyncItemWriter<SalesInfo> asyncItemWriter,
            TaskExecutor importJobTaskExecutor,
            CustomSkipPolicy customSkipPolicy,
            CustomStepExecutionListener customStepExecutionListener
    ) {
        return new StepBuilder("fromFileIntoDatabase")
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .<SalesInfoDTO, Future<SalesInfo>>chunk(100)
                .reader(salesInfoReader)
                .processor(asyncItemProcessor)
                .writer(asyncItemWriter)
                .faultTolerant()
                .skipPolicy(customSkipPolicy)
                .listener(customStepExecutionListener)
                .taskExecutor(importJobTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    FlatFileItemReader<SalesInfoDTO> salesInfoReader(@Value("#{jobParameters['input.file.name']}") String resource) {
        String[] names = new String[]{
                "product",
                "seller",
                "sellerId",
                "price",
                "city",
                "category"
        };

        return new FlatFileItemReaderBuilder<SalesInfoDTO>()
                .resource(new FileSystemResource(resource))
                .name("salesInfoReader")
                .delimited()
                .delimiter(",")
                .names(names)
                .linesToSkip(1)
                .targetType(SalesInfoDTO.class)
                .build();
    }

    @Bean
    ItemWriter<SalesInfo> salesInfoDatabaseWriter() {
        return new JpaItemWriterBuilder<SalesInfo>()
                .entityManagerFactory(entityMangerFactory)
                .build();
    }

    @Bean
    TaskExecutor importJobTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        threadPoolTaskExecutor.setCorePoolSize(5);
        threadPoolTaskExecutor.setMaxPoolSize(5);
        threadPoolTaskExecutor.setQueueCapacity(10);
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.setThreadNamePrefix("Thread JOB -> :");

        return threadPoolTaskExecutor;
    }

    @Bean
    AsyncItemProcessor<SalesInfoDTO, SalesInfo> asyncItemProcessor(SalesInfoProcessor salesInfoProcessor, TaskExecutor importJobTaskExecutor) {
        var asyncItemProcessor = new AsyncItemProcessor<SalesInfoDTO, SalesInfo>();
        asyncItemProcessor.setDelegate(salesInfoProcessor);
        asyncItemProcessor.setTaskExecutor(importJobTaskExecutor);

        return asyncItemProcessor;
    }

    @Bean
    AsyncItemWriter<SalesInfo> asyncItemWriter(ItemWriter<SalesInfo> salesInfoItemWriter) {
        var asyncItemWriter = new AsyncItemWriter<SalesInfo>();
        asyncItemWriter.setDelegate(salesInfoItemWriter);

        return asyncItemWriter;
    }
}
