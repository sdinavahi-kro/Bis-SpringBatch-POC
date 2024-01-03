package com.srini.SpringBatchPOC.config;

import com.srini.SpringBatchPOC.model.Student;
import com.srini.SpringBatchPOC.processor.CustomItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;


import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
public class BatchConfig {

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcCursorItemReader<Student> reader(){
        JdbcCursorItemReader<Student> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from dbo.t_Student");
        reader.setRowMapper(new RowMapper<>() {
            @Override
            public Student mapRow(ResultSet rs, int rowNum) throws SQLException {
                Student stu = new Student();
                stu.setStdId(rs.getString("Std_Id"));
                stu.setName(rs.getString("Name"));
                return stu;
            }
        });
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Student> writer(){
        FlatFileItemWriter<Student> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("/Users/sd33930/POC/SpringBatch-test.csv"));
        DelimitedLineAggregator<Student> aggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<Student> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"stdId","name"});
        aggregator.setFieldExtractor(fieldExtractor);
        writer.setLineAggregator(aggregator);
        return writer;
    }

    @Bean
    public ItemProcessor<Student, Student> itemProcessor() {
        return new CustomItemProcessor();
    }

    @Bean
    protected Step step1(JobRepository jobRepository,
                         PlatformTransactionManager transactionManager
                         ) {
        return new StepBuilder("step1", jobRepository).<Student, Student> chunk(100, transactionManager)
                .reader(reader()).processor(itemProcessor()).writer(writer()).build();
    }

    @Bean(name ="firstBatchJob")
    public Job job(JobRepository jobRepository, Step step1) {
        return new JobBuilder("firstBatchJob", jobRepository).incrementer(new RunIdIncrementer()).flow(step1).end().build();
    }

}

