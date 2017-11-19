package com.camendoza94.healtcheck;

import com.camendoza94.zoo.ZooKeeperClientManager;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.camendoza94.zoo.ZooKeeperController.BASE_PATH;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class DirectoryHealthCheck implements Job {

    private static final Integer HEALTH_CHECK_FREQUENCY_SECONDS = 90;
    private static final String HEALTH = "/health";
    private static final String UP = "UP";
    private final ZooKeeperClientManager zooKeeperClientManager = new ZooKeeperClientManager();

    @Override
    public void execute(JobExecutionContext jec) throws JobExecutionException {
        try {
            System.out.println("Start ServiceHealth Check");
            RestTemplate template = new RestTemplate();
            List<String> children = zooKeeperClientManager.getZNodeTree(BASE_PATH);
            for (String child : children) {
                String URL = "http://" + obtainBaseEndpoint(child);
                URL += HEALTH;
                try {
                    ResponseEntity<ServiceHealth> response = template.getForEntity(URL, ServiceHealth.class);
                    if (UP.equals(response.getBody().getStatus()))
                        zooKeeperClientManager.update(child, new byte[]{(byte) 1});
                    else
                        zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                } catch (Exception e) {
                    //Server is down or could not complete request correctly
                    zooKeeperClientManager.update(child, new byte[]{(byte) 0});
                }
            }
            System.out.println("ServiceHealth Check Done");
        } catch (Exception ex) {
            Logger.getLogger(DirectoryHealthCheck.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private static String obtainBaseEndpoint(String path) throws Exception {
        int start = path.indexOf("/", BASE_PATH.length() + 2);
        if (start != -1) {
            int end = path.indexOf("/", start + 1);
            if (end != -1)
                return path.substring(start + 1, end);
        }
        throw new Exception("No instances in Zookeeper registry.");
    }


    public static void startHealthCheckJob() {
        try {
            // Grab the Scheduler instance from the Factory
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            // and start it off
            scheduler.start();
            // define the job and tie it to our Job class
            JobDetail job = newJob(DirectoryHealthCheck.class)
                    .withIdentity("microserviceheartbeat", "heartbeats")
                    .build();

            // Trigger the job to run now, and then repeat every 90 seconds
            Trigger trigger = newTrigger()
                    .withIdentity("microserviceHeartbeatTrigger", "DefaultTrigger")
                    .startNow()
                    .withSchedule(simpleSchedule()
                            .withIntervalInSeconds(HEALTH_CHECK_FREQUENCY_SECONDS)
                            .repeatForever())
                    .build();

            // Tell quartz to schedule the job using our trigger
            scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException ex) {
            Logger.getLogger(DirectoryHealthCheck.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}