package com.github.siwonpawel.batch.salesinfo;

import com.github.siwonpawel.batch.salesinfo.dto.SalesInfoDTO;
import com.github.siwonpawel.domain.SalesInfo;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManagerFactory;

@Configuration
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
    Step fromFileIntoDatabase(ItemReader<SalesInfoDTO> salesInfoReader, ItemProcessor<SalesInfoDTO, SalesInfo> salesInfoProcessor, ItemWriter<SalesInfo> salesInfoDatabaseWriter) {
        return new StepBuilder("fromFileIntoDatabase")
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .<SalesInfoDTO, SalesInfo>chunk(100)
                .reader(salesInfoReader)
                .processor(salesInfoProcessor)
                .writer(salesInfoDatabaseWriter)
                .build();
    }

    @Bean
    ItemReader<SalesInfoDTO> salesInfoReader() {
        String[] names = new String[]{
                "product",
                "seller",
                "sellerId",
                "price",
                "city",
                "category"
        };

        return new FlatFileItemReaderBuilder<SalesInfoDTO>()
                .resource(new ClassPathResource("data/Pascoal-Store.csv"))
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

}
