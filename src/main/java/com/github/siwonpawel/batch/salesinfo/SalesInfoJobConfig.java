package com.github.siwonpawel.batch.salesinfo;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.batch.salesinfo.faulttolerance.CustomSkipPolicy;
import com.github.siwonpawel.batch.salesinfo.listeners.CustomJobExecutionListener;
import com.github.siwonpawel.batch.salesinfo.listeners.CustomStepExecutionListener;
import com.github.siwonpawel.batch.salesinfo.processor.SalesInfoProcessor;
import com.github.siwonpawel.batch.salesinfo.step.FileCollector;
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
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.Future;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SalesInfoJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final FileCollector fileCollector;

    @Bean
    Job importSalesInfo(Step fromFileIntoKafka, Step fileCollectorTasklet, CustomJobExecutionListener customJobExecutionListener) {
        return new JobBuilder("importSalesInfo")
                .repository(jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(fromFileIntoKafka)
                .next(fileCollectorTasklet)
                .listener(customJobExecutionListener)
                .build();
    }

    @Bean
    Step fromFileIntoKafka(
            SynchronizedItemStreamReader<SalesInfoDTO> salesInfoReader,
            AsyncItemProcessor<SalesInfoDTO, SalesInfo> asyncItemProcessor,
            AsyncItemWriter<SalesInfo> asyncItemWriter,
            CustomSkipPolicy customSkipPolicy,
            CustomStepExecutionListener customStepExecutionListener,
            TaskExecutor importJobTaskExecutor
    ) {
        return new StepBuilder("fromFileIntoKafka")
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
    SynchronizedItemStreamReader<SalesInfoDTO> salesInfoReader(@Value("#{jobParameters['input.file.name']}") String resource) {
        String[] names = new String[]{
                "product",
                "seller",
                "sellerId",
                "price",
                "city",
                "category"
        };

        var salesInfoReader = new FlatFileItemReaderBuilder<SalesInfoDTO>()
                .resource(new FileSystemResource(resource))
                .name("salesInfoReader")
                .delimited()
                .delimiter(",")
                .names(names)
                .linesToSkip(1)
                .targetType(SalesInfoDTO.class)
                .build();

        return new SynchronizedItemStreamReaderBuilder<SalesInfoDTO>()
                .delegate(salesInfoReader)
                .build();
    }

    @Bean
    AsyncItemWriter<SalesInfo> salesInfoDatabaseWriter(KafkaTemplate<String, SalesInfo> kafkaTemplate) {
        KafkaItemWriter<String, SalesInfo> itemWriter = new KafkaItemWriterBuilder<String, SalesInfo>()
                .itemKeyMapper(item -> String.valueOf(item.getSellerId()))
                .kafkaTemplate(kafkaTemplate)
                .delete(false)
                .build();

        var asyncItemWriter = new AsyncItemWriter<SalesInfo>();
        asyncItemWriter.setDelegate(itemWriter);

        return asyncItemWriter;
    }

    @Bean
    AsyncItemProcessor<SalesInfoDTO, SalesInfo> asyncItemProcessor(SalesInfoProcessor salesInfoProcessor, TaskExecutor importJobTaskExecutor) {
        var asyncItemProcessor = new AsyncItemProcessor<SalesInfoDTO, SalesInfo>();
        asyncItemProcessor.setDelegate(salesInfoProcessor);
        asyncItemProcessor.setTaskExecutor(importJobTaskExecutor);


        return asyncItemProcessor;
    }

    @Bean
    public TaskExecutor importJobTaskExecutor() {
        return new SyncTaskExecutor();
    }

    @Bean
    Step fileCollectorTasklet() {
        return new StepBuilder("fileCollector")
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .tasklet(fileCollector)
                .build();
    }
}
