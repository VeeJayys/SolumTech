package com.Solum.Solumdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
//import org.springframework.batch.item.support.builder.RepositoryItemWriterBuilder;

import java.io.File;
import java.nio.file.*;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    @Bean
    public Job importUserJob(JobBuilderFactory jobBuilderFactory, Step step1, Step step2) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1)
                .next(step2)
                .end()
                .build();
    }

    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, FlatFileItemReader<User> reader, UserItemProcessor processor, UserItemWriter writer) {
        return stepBuilderFactory.get("step1")
                .<User, User>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(new TimeLoggingStepListener())
                .build();
    }

    @Bean
    public Step step2(StepBuilderFactory stepBuilderFactory, Tasklet moveFileTasklet) {
        return stepBuilderFactory.get("step2")
                .tasklet(moveFileTasklet)
                .listener(new TimeLoggingStepListener())
                .build();
    }

    @Bean
    public FlatFileItemReader<User> reader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("userItemReader")
                .resource(new FileSystemResource(new File("./input/input.csv")))
                .delimited()
                .names("id", "name", "email")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(User.class);
                }})
                .build();
    }

    @Bean
    public UserItemProcessor processor() {
        return new UserItemProcessor();
    }

    @Bean
    public UserItemWriter writer(UserRepository repository) {
        return new UserItemWriter(repository);
    }

    @Bean
    public Tasklet moveFileTasklet() {
        return (contribution, chunkContext) -> {
            Path source = Paths.get("./input/input.csv");
            Path target = Paths.get("./backup/input.csv");
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            return RepeatStatus.FINISHED;
        };
    }

    public static class TimeLoggingStepListener extends StepExecutionListenerSupport {
        private long startTime;

        @Override
        public void beforeStep(StepExecution stepExecution) {
            startTime = System.currentTimeMillis();
        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            long endTime = System.currentTimeMillis();
            log.info("Step {} took {} ms", stepExecution.getStepName(), (endTime - startTime));
            return ExitStatus.COMPLETED;
        }
    }
}
