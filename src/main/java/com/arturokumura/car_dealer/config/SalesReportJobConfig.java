package com.arturokumura.car_dealer.config;

import com.arturokumura.car_dealer.domain.SaleRecord;
import com.arturokumura.car_dealer.writer.SalesReportWriter;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.file.MultiResourceItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration //Sinalizar que a classe serve para configurar algo
public class SalesReportJobConfig {

    @Bean //Instância de uma classe - Injeção de dependência
    public Job salesReportJob(JobRepository jobRepository, Step salesReportStep) {
        return new JobBuilder("salesReportJob", jobRepository) //Chama o job para gerar o relatório
                .start(salesReportStep)
                .build();
    }

    @Bean
    public Step salesReportStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                MultiResourceItemReader<SaleRecord> saleReader,
                                ItemProcessor<SaleRecord, SaleRecord> saleProcessor,
                                SalesReportWriter writer) {
        return new StepBuilder("salesReportStep", jobRepository)
                .<SaleRecord, SaleRecord>chunk(100) // chunck: menor parte do batch
                .reader(saleReader)
                .processor(saleProcessor)
                .writer(writer)
                .transactionManager(transactionManager)
                .build();
    }
}

