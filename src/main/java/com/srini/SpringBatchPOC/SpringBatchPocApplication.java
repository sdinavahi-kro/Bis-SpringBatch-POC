package com.srini.SpringBatchPOC;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class SpringBatchPocApplication {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	Job firstBatchJob;

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchPocApplication.class, args);
		System.out.println("Hi");
	}

	@Scheduled(cron = "0 */1 * * * ?")
	public void perform() throws Exception
	{
		JobParameters params = new JobParametersBuilder()
				.addString("JobID",
						String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
		jobLauncher.run(firstBatchJob, params);
	}

}
