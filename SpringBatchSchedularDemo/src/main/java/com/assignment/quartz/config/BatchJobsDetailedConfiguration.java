package com.assignment.quartz.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.ApplicationContextFactory;
import org.springframework.batch.core.configuration.support.GenericApplicationContextFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.JobDetailFactoryBean;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import com.assignment.config.JobsConfig;

@Configuration
@PropertySource("classpath:batch-hsql.properties")
public class BatchJobsDetailedConfiguration {
	
	Logger logger = LoggerFactory.getLogger(BatchJobsDetailedConfiguration.class);
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Value( "${batch.job.scheduleName}" )
	private String jobScheduleName;
	
	@Autowired
    private JdbcTemplate jdbcTemplate;

	@Bean(name = "reportsDetailContext")
	public ApplicationContextFactory getApplicationContext() {
		return new GenericApplicationContextFactory(JobsConfig.class);
	}

	@Bean(name = "reportsDetailJob")
	public JobDetailFactoryBean jobDetailFactoryBean() {
		JobDetailFactoryBean jobDetailFactoryBean = new JobDetailFactoryBean();
		jobDetailFactoryBean.setJobClass(JobLauncherDetails.class);
		jobDetailFactoryBean.setDurability(true);
		Map<String, Object> map = new HashMap<>();
		map.put("jobLauncher", jobLauncher);
		map.put("jobName", JobsConfig.jobName);
		jobDetailFactoryBean.setJobDataAsMap(map);
		return jobDetailFactoryBean;
	}

	@Bean(name = "reportsCronJob")
	public CronTriggerFactoryBean cronTriggerFactoryBean() {
		logger.info("Schedule name is: " + jobScheduleName);
		String schedule = jdbcTemplate.queryForList("SELECT SCHEDULE_EXPRESSION FROM SCHEDULING_INFO WHERE SCHEDULE_NAME = '"+jobScheduleName+"'", String.class )
							.get(0);
		logger.info("Schedule is: " + schedule);
		CronTriggerFactoryBean cronTriggerFactoryBean = new CronTriggerFactoryBean();
		cronTriggerFactoryBean.setJobDetail(jobDetailFactoryBean().getObject());
		cronTriggerFactoryBean.setCronExpression(schedule);
		return cronTriggerFactoryBean;
	}

	@Bean
	public SchedulerFactoryBean schedulerFactoryBean(JobRegistry jobRegistry) throws NoSuchJobException {
		SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
		schedulerFactoryBean.setTriggers(cronTriggerFactoryBean().getObject());
		schedulerFactoryBean.setAutoStartup(true);
		Map<String, Object> map = new HashMap<>();
		map.put("jobLauncher", jobLauncher);
		map.put("jobLocator", jobRegistry);
		schedulerFactoryBean.setSchedulerContextAsMap(map);
		schedulerFactoryBean.setTaskExecutor(Executors.newSingleThreadExecutor());
		return schedulerFactoryBean;
	}
	
	@Bean
	public JobOperator jobOperator(final JobLauncher jobLauncher, final JobRepository jobRepository,
	        final JobRegistry jobRegistry, final JobExplorer jobExplorer) {
	    final SimpleJobOperator jobOperator = new SimpleJobOperator();
	    jobOperator.setJobLauncher(jobLauncher);
	    jobOperator.setJobRepository(jobRepository);
	    jobOperator.setJobRegistry(jobRegistry);
	    jobOperator.setJobExplorer(jobExplorer);
	    return jobOperator;
	}

	@Bean
	public JobExplorer jobExplorer(final DataSource dataSource) throws Exception {
	    final JobExplorerFactoryBean bean = new JobExplorerFactoryBean();
	    bean.setDataSource(dataSource);
	    bean.setTablePrefix("BATCH_");
	    bean.setJdbcOperations(jdbcTemplate);
	    bean.afterPropertiesSet();
	    return bean.getObject();
	}
}