package com.joaodev.ordersync.batch;

import com.joaodev.ordersync.domain.OrderData;
import com.joaodev.ordersync.repository.OrderRepository;
import com.joaodev.ordersync.service.OrderVersioningService;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;

import org.springframework.batch.infrastructure.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class OrderImportJobConfig {

    @Bean
    @StepScope
    public FlatFileItemReader<OrderCsvRecord> orderCsvReader(
            @Value("#{jobParameters['filePath']}") String filePath) {

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("legacyOderId", "customerName", "productCode", "quantity", "unitPrice", "status");

        BeanWrapperFieldSetMapper<OrderCsvRecord> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(OrderCsvRecord.class);

        DefaultLineMapper<OrderCsvRecord> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        FlatFileItemReader<OrderCsvRecord> reader =
                new FlatFileItemReader<>(new FileSystemResource(filePath), lineMapper);
        reader.setLinesToSkip(1);
        return reader;
    }

    @Bean
    public ItemProcessor<OrderCsvRecord, OrderData> orderCsvProcessor() {
        return csvRecord -> {
            if (csvRecord.getLegacyOrderId() == null
                || csvRecord.getCustomerName() == null
                || csvRecord.getCustomerName().isBlank()) {
                return null;
            }
            return new OrderData(
                    csvRecord.getLegacyOrderId(),
                    csvRecord.getCustomerName(),
                    csvRecord.getProductCode(),
                    csvRecord.getQuantity(),
                    csvRecord.getUnitPrice(),
                    csvRecord.getStatus()
            );
        };
    }

    @Bean
    public ItemWriter<OrderData> orderCsvWriter(OrderRepository orderRepository,
                                                OrderVersioningService versioningService)  {
        return chunk -> {
            for (OrderData data : chunk) {
                boolean exists = orderRepository.findByLegacyOrderId(data.legacyOrderId()).isPresent();
                if (exists) {
                    versioningService.updateOrder(data.legacyOrderId(), data, "FILE_WATCHER");
                } else {
                    versioningService.createOrder(data, "FILE_WATCHER");
                }
            }
        };
    }

    @Bean
    public Step orderImportStep(JobRepository jobRepository,
                                @Qualifier("transactionManager") PlatformTransactionManager transactionManager,
                                FlatFileItemReader<OrderCsvRecord> orderCsvReader,
                                ItemProcessor<OrderCsvRecord, OrderData> orderCsvProcessor,
                                ItemWriter<OrderData> orderCsvWriter) {
        return new StepBuilder("orderImportStep", jobRepository)
                .<OrderCsvRecord, OrderData>chunk(5)
                .transactionManager(transactionManager)
                .reader(orderCsvReader)
                .processor(orderCsvProcessor)
                .writer(orderCsvWriter)
                .build();
    }

    @Bean
    public Job orderImportJob(JobRepository jobRepository, Step orderImportStep) {
        return new JobBuilder("orderImportJob", jobRepository)
                .start(orderImportStep)
                .build();
    }
}
