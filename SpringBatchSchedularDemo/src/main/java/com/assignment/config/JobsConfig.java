package com.assignment.config;

import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

@Configuration
@EnableBatchProcessing
public class JobsConfig {

	Logger logger = LoggerFactory.getLogger(JobsConfig.class);
	
	@Autowired
    private JobRegistry jobRegistry;
	
    public final static String jobName = "ChainedStepsJob";
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private TaskletStep taskletStep1() {
    	return stepBuilderFactory.get("JOB_STEP1").allowStartIfComplete(true).tasklet((contribution, chunkContext) -> {
        	logger.info("JOB_STEP1" + " Executed by Thread: "+ Thread.currentThread().getName());
        	String execCondition = jdbcTemplate.queryForList("SELECT EXECUTE_CONDITION FROM STEP_EXECUTION_CONDITION WHERE STEP_NAME = 'JOB_STEP1'", String.class )
					.get(0);
        	logger.debug("Execution condition for Step 'JOB_STEP1' is: " + execCondition);
        	if(StepExecutionCondition.STOP.getName().equals(execCondition)) {
        		//throw new RuntimeException("JOB_STEP1 is Stopped");
        		contribution.setExitStatus(ExitStatus.FAILED);
        		Thread.sleep(15000);
        		return RepeatStatus.CONTINUABLE;
        	}
            logger.info("JOB_STEP1 has finished execution");
            return RepeatStatus.FINISHED;
        }).build();

    }
    
    private TaskletStep taskletStep2() {
        return stepBuilderFactory.get("JOB_STEP2").allowStartIfComplete(true).tasklet((contribution, chunkContext) -> {
        	logger.info("JOB_STEP2" + " Executed by Thread: "+ Thread.currentThread().getName());
        	String execCondition = jdbcTemplate.queryForList("SELECT EXECUTE_CONDITION FROM STEP_EXECUTION_CONDITION WHERE STEP_NAME = 'JOB_STEP2'", String.class )
					.get(0);
        	logger.debug("Execution condition for Step 'JOB_STEP2' is: " + execCondition);
        	if(StepExecutionCondition.STOP.getName().equals(execCondition)) {
        		//throw new RuntimeException("JOB_STEP2 is Stopped");
        		contribution.setExitStatus(ExitStatus.FAILED);
        		Thread.sleep(15000);
        		return RepeatStatus.CONTINUABLE;
        	}
            logger.info("JOB_STEP2 has finished execution");
            return RepeatStatus.FINISHED;
        }).build();

    }
    
    private TaskletStep taskletStep3() {
    	return stepBuilderFactory.get("JOB_STEP3").allowStartIfComplete(true).tasklet((contribution, chunkContext) -> {
        	logger.info("JOB_STEP3" + " Executed by Thread: "+ Thread.currentThread().getName());
        	String execCondition = jdbcTemplate.queryForList("SELECT EXECUTE_CONDITION FROM STEP_EXECUTION_CONDITION WHERE STEP_NAME = 'JOB_STEP3'", String.class )
					.get(0);
        	logger.debug("Execution condition for Step 'JOB_STEP3' is: " + execCondition);
        	if(StepExecutionCondition.STOP.getName().equals(execCondition)) {
        		//throw new RuntimeException("JOB_STEP3 is Stopped");
        		contribution.setExitStatus(ExitStatus.FAILED);
        		Thread.sleep(15000);
        		return RepeatStatus.CONTINUABLE;
        	}
            logger.info("JOB_STEP3 has finished execution");
            return RepeatStatus.FINISHED;
        }).build();

    }
    
    @Bean
    public Job chainedStepsJob() {
    	
    	Flow flow1 = new FlowBuilder<SimpleFlow>("flow1").start(taskletStep1()).build();
        Flow flow2 = new FlowBuilder<SimpleFlow>("flow2").start(taskletStep2()).build();
        Flow flow3 = new FlowBuilder<SimpleFlow>("flow3").start(taskletStep3()).build();
        
    	return (jobBuilderFactory.get("ChainedStepsJob")
                .incrementer(new RunIdIncrementer())
                .start(flow1)
                .next(flow2)
                .next(flow3)
                .build()).build();
    }
    
    /*@Bean
    public DataSource dataSource() {
     final DriverManagerDataSource dataSource = new DriverManagerDataSource();
     dataSource.setDriverClassName("com.mysql.jdbc.Driver");
     dataSource.setUrl("jdbc:mysql://localhost/testdb");
     dataSource.setUsername("root");
     dataSource.setPassword("root");
     
     return dataSource;
    } */
    
    @Bean
	public DataSource dataSource(){
		EmbeddedDatabaseBuilder embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();
		return embeddedDatabaseBuilder
				.addScript("classpath:org/springframework/batch/core/schema-drop-hsqldb.sql")
				.addScript("classpath:org/springframework/batch/core/schema-hsqldb.sql")
				.addScript("classpath:scheduling-info-drop.sql")
				.addScript("classpath:scheduling-info.sql")
				.setType(EmbeddedDatabaseType.HSQL)
				.build();
	}
}
