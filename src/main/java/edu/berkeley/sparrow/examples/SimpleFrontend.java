/*
 * Copyright 2013 The Regents of The University California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.sparrow.examples;

import edu.berkeley.sparrow.api.SparrowFrontendClient;
import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.scheduler.SchedulerThrift;
import edu.berkeley.sparrow.daemon.util.*;
import edu.berkeley.sparrow.thrift.FrontendService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Simple frontend that runs jobs composed of sleep tasks.
 */
public class SimpleFrontend implements FrontendService.Iface {
    /**
     * Amount of time to launch tasks for.
     */
    public static final String EXPERIMENT_S = "experiment_s";
    public static final int DEFAULT_EXPERIMENT_S = 300;

    public static final String JOB_ARRIVAL_PERIOD_MILLIS = "job_arrival_period_millis";
    public static final int DEFAULT_JOB_ARRIVAL_PERIOD_MILLIS = 100;

    /**
     * Number of tasks per job.
     */
    public static final String TASKS_PER_JOB = "tasks_per_job";
    public static final int DEFAULT_TASKS_PER_JOB = 1;

    //Sending this extra tasks so that the runnable doesn't crash because
    //it keeps fetching from taskDuration arraylist until the experiment duration is completed.
    //We can't be very precise.
    public static final int EXTRA_TASKS = 500;

    /**
     * Number of tasks per job.
     */
    public static final String TOTAL_NO_OF_TASKS = "total_no_of_tasks";
    public static final int DEFAULT_TOTAL_NO_OF_TASKS = 3500;

    public static final String LOAD = "load";
    public static final double DEFAULT_LOAD = 0.1;

    public static final String FAKE_TASKS = "fake_tasks";
    public static final int DEFAULT_FAKE_TASKS = 0;

    public static final String CHANGE_ARRIVAL = "change_arrival";
    public static final int DEFAULT_CHANGE_ARRIVAL = 0;

    public static final String FAKE_LOAD_RATIO = "fake_load_ratio";
    public static final String DEFAULT_FAKE_LOAD_RATIO = "0.1:0.2";

    public static final String FAKE_LOADS = "fake_loads";
    public static final String DEFAULT_FAKE_LOADS = "0.1";
    public static final String REAL_LOADS = "real_loads";
    public static final String DEFAULT_REAL_LOADS = "0.2";

    private static final String ARRIVAL_CONFIG_TIME = "arrival_config_time";
    private static final int DEFAULT_ARRIVAL_CONFIG_TIME = 60;
    private static int arrivalConfigTime;


    /**
     * Duration of one task, in milliseconds
     */
    public static final String TASK_DURATION_MILLIS = "task_duration_millis";
    public static final int DEFAULT_TASK_DURATION_MILLIS = 100;

    private static String SLAVES = "slaves";

    /**
     * Host and port where scheduler is running.
     */
    public static final String SCHEDULER_HOST = "scheduler_host";
    public static final String DEFAULT_SCHEDULER_HOST = "localhost";
    public static final String SCHEDULER_PORT = "scheduler_port";

    /**
     * Default application name.
     */
    public static final String APPLICATION_ID = "sleepApp";

    private static final Logger LOG = Logger.getLogger(SimpleFrontend.class);

    private static final TUserGroupInfo USER = new TUserGroupInfo();

    private SparrowFrontendClient client;

    private final static Logger AUDIT_LOG = Logging.getCustomAuditLogger(SimpleFrontend.class);

    //Worker Speed Mapped to its corresponding worker
    public static Map<String, String> workSpeedMap = new HashMap<String, String>();
    //To pass different task duration to the runnable.
    static ArrayList<Double> taskDurations = new ArrayList<Double>();
    static ArrayList<Double> taskDurationsFake = new ArrayList<Double>();
    public static Random random = new Random();

    //Get data from exponential Distribution with parameter lambda
    public static double getNext(double lambda) {
        return Math.log(1 - random.nextDouble()) / (-lambda);
    }

    /******** For Zipf's Distribution Not currently being used ****************/

    public static int RATIO_BETWEEN_MAX_MIN = 100;
    public static double upper_bound = 1.0;
    public static double lower_bound = upper_bound / RATIO_BETWEEN_MAX_MIN;

    //Avoiding Max value in the worker = min value of worker speed
    public static int[] unidenticalWorkSpeeds(int no_of_elements, int exponent) {
        ZipfDistribution zipfDistribution = new ZipfDistribution(no_of_elements, exponent);
        int[] worker_speeds = zipfDistribution.sample(no_of_elements);
        int max = MinMax.getMaxValue(worker_speeds);
        int min = MinMax.getMinValue(worker_speeds);
        if (max == min) {
            worker_speeds = unidenticalWorkSpeeds(no_of_elements, exponent);
        }
        return worker_speeds;
    }
    /******** For Zipf's Distribution Not currently being used ****************/

    /**
     * A runnable which Spawns a new thread to launch a scheduling request.
     */
    private class JobLaunchRunnable implements Runnable {
        private int tasksPerJob;
        private ArrayList<Double> taskDurations;
        private int i;
        private String workSpeedMap;
        private boolean isFake;

        public JobLaunchRunnable(int tasksPerJob, ArrayList<Double> taskDurations, String workSpeedMap) {
            this.tasksPerJob = tasksPerJob;
            this.taskDurations = taskDurations;
            this.i = 0; //index
            this.workSpeedMap = workSpeedMap;
        }

        public void setFake(boolean isFake) {
            this.isFake = isFake;
        }

        public boolean getFake() {
            return this.isFake;
        }

        @Override
        public void run() {
            try {
                //thisHost = Inet4Address.getLocalHost().getHostAddress();


                // Generate tasks in the format expected by Sparrow. First, pack task parameters.
                ByteBuffer message = ByteBuffer.allocate(16);
                //Sending this to double confirm response time and waiting time
                message.putLong(System.currentTimeMillis());
                //Get Different Task Durations from Exp Distribution
                message.putDouble(taskDurations.get(i));
                i++;

                List<TTaskSpec> tasks = new ArrayList<TTaskSpec>();
                for (int taskId = 0; taskId < tasksPerJob; taskId++) {
                    TTaskSpec spec = new TTaskSpec();
                    spec.setTaskId(Integer.toString(taskId));
                    spec.setMessage(message.array());
                    tasks.add(spec);
                }
                long start = System.currentTimeMillis();
                LOG.debug("sunilmdhr" + ":" + "0" + ":"
                        + "frontend_job_submitted" + ":" + "1" + ":" + System.currentTimeMillis());
                
                try {
                    client.submitJob(APPLICATION_ID, tasks, USER, workSpeedMap, isFake);
                } catch (TException e) {
                    LOG.error("Scheduling request failed!", e);
                }
                long end = System.currentTimeMillis();
                LOG.debug("Scheduling request duration " + (end - start));
            } catch (Exception e) {
                LOG.debug(e.getMessage());
            }
        }
    }


    public void run(String[] args) {
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("c", "configuration file").withRequiredArg().ofType(String.class);
            parser.accepts("w", "configuration file").
                    withRequiredArg().ofType(String.class);
            parser.accepts("help", "print help statement");
            OptionSet options = parser.parse(args);
            //Logging.configureAuditLoggingNew();
            if (options.has("help")) {
                parser.printHelpOn(System.out);
                System.exit(-1);
            }

            // Logger configuration: log to the console
            BasicConfigurator.configure();
            LOG.setLevel(Level.DEBUG);

            Configuration conf = new PropertiesConfiguration();

            if (options.has("c")) {
                String configFile = (String) options.valueOf("c");
                conf = new PropertiesConfiguration(configFile);
            }

            //Gives us the private IP addresses
            Set<InetSocketAddress> backends = ConfigUtil.parseBackends(conf);
            int total_workers = backends.size();

            //Use this flag to retrieve the backend and worker speed mapping
            Configuration slavesConfig = new PropertiesConfiguration();
            if (options.has("w")) {
                String configFile = (String) options.valueOf("w");
                try {
                    slavesConfig = new PropertiesConfiguration(configFile);
                } catch (ConfigurationException e) {
                }
            }

            if (!slavesConfig.containsKey(SLAVES)) {
                throw new RuntimeException("Missing configuration node monitor list");
            }

            //Creates a string which can be parse to compare with respective host IP
            String workSpeed = "";
            for (String node : slavesConfig.getStringArray(SLAVES)) {
                workSpeed = workSpeed + node + ",";
            }

            //The workspeed.txt file contains list of worker speeds. They're mapped to the ip addresses available
            Properties props = new Properties();
            props.load(new StringReader(workSpeed.replace(",", "\n")));
            //Could have use the properties object but trying to make it consistent with other code.
            ArrayList<Double> workerSpeed = new ArrayList<Double>();
            for (Map.Entry<Object, Object> e : props.entrySet()) {
                workerSpeed.add(Double.valueOf(e.getValue().toString()));
                workSpeedMap.put(e.getKey().toString(), e.getValue().toString());
            }

            //We don't necessarilty have to define the experiment duration. Just need to make sure that we pass
            //more task duration so that task launcher doesn't crash. Note that, even if the taskLauncher crashes,
            //it doesn't really affect the logs that we're trying to fetch.
            int experimentDurationS = conf.getInt(EXPERIMENT_S, DEFAULT_EXPERIMENT_S);
            int taskDurationMillis = conf.getInt(TASK_DURATION_MILLIS, DEFAULT_TASK_DURATION_MILLIS);
            double load = conf.getDouble(LOAD, DEFAULT_LOAD);
            int totalNoOfTasks = conf.getInt(TOTAL_NO_OF_TASKS, DEFAULT_TOTAL_NO_OF_TASKS);
            int tasksPerJob = conf.getInt(TASKS_PER_JOB, DEFAULT_TASKS_PER_JOB);
            int fakeTasks = conf.getInt(FAKE_TASKS, DEFAULT_FAKE_TASKS);
            int changeArrival = conf.getInt(CHANGE_ARRIVAL, DEFAULT_CHANGE_ARRIVAL);
            String realLoads = "";
            String fakeLoads = "";
            for (String altered : conf.getStringArray(REAL_LOADS)) {
                realLoads = realLoads + altered + ",";
            }
            for (String altered : conf.getStringArray(FAKE_LOADS)) {
                fakeLoads = realLoads + altered + ",";
            }
            //String realLoads = conf.getString(REAL_LOADS, DEFAULT_REAL_LOADS);
            // String fakeLoads = conf.getString(FAKE_LOADS, DEFAULT_FAKE_LOADS);
            arrivalConfigTime = conf.getInt(ARRIVAL_CONFIG_TIME, DEFAULT_ARRIVAL_CONFIG_TIME);

            String fakeLoadRatio = conf.getString(FAKE_LOAD_RATIO, DEFAULT_FAKE_LOAD_RATIO);

            int schedulerPort = conf.getInt(SCHEDULER_PORT,
                    SchedulerThrift.DEFAULT_SCHEDULER_THRIFT_PORT);
            String schedulerHost = conf.getString(SCHEDULER_HOST, DEFAULT_SCHEDULER_HOST);
            client = new SparrowFrontendClient();
            client.initialize(new InetSocketAddress(schedulerHost, schedulerPort), APPLICATION_ID, this);
            ScheduledThreadPoolExecutor taskLauncher = null;

            HashMap<Integer, Double> mapRealLoad = new HashMap<Integer, Double>();
            HashMap<Integer, Double> mapFakeLoad = new HashMap<Integer, Double>();


            double W = 0; //W is total Worker Speed in the system
            for (double m : workerSpeed) {
                W += m;
            }

            //Generate Exponential Data
            int median_task_duration = taskDurationMillis;
            double lambda = 1.0 / median_task_duration;
            random.setSeed(123456789);
            //keep producing tasks, we'll cut off based on how long we want to run the experiment
            for (int l = 0; l < totalNoOfTasks + EXTRA_TASKS; l++) { //Added extra tasks
                taskDurations.add(getNext(lambda));
            }

            for (int lf = totalNoOfTasks; lf < (totalNoOfTasks * 2) + EXTRA_TASKS; lf++) { //Added extra tasks
                taskDurationsFake.add(getNext(lambda));
            }
            double averageTaskDurationMillis = median_task_duration;
            double serviceRate = W / averageTaskDurationMillis; //When taking 3500 tasks with the given seed for exponential distribution, this is the average we get for task duration


            double realLoad = 0.1;
            double fakeLoad = 0.2;
            long arrivalPeriodMillisFake;
            long arrivalPeriodMillisReal;
            long arrivalPeriodMillis;
            if (changeArrival == 0) {
                String[] entry = fakeLoadRatio.split(":");
                if (entry.length == 2) {
                    realLoad = Double.valueOf(entry[0].trim());
                    fakeLoad = Double.valueOf(entry[1].trim());
                    if (fakeLoad + realLoad >= 1) {
                        LOG.debug("Warning Load >=1");
                    }
                } else {
                    LOG.debug("Warning!!! Using default Load");
                    realLoad = 0.1;
                    fakeLoad = 0.2;
                }


                if (fakeTasks == 1) {
                    LOG.debug("Enabling Fake Tasks");
                    double arrivalRateReal = realLoad * serviceRate;
                    double arrivalRateFake = fakeLoad * serviceRate;

                    //Get Arrival Period for individual task
                    arrivalPeriodMillisReal = (long) (tasksPerJob / arrivalRateReal);
                    arrivalPeriodMillisFake = (long) (tasksPerJob / arrivalRateFake);

                    //Get Experiment duration based on no. of tasks (in s)
                    //Need to add more seconds to make sure all tasks get executed
                    //TODO fix this later
                    experimentDurationS = (int) ((totalNoOfTasks * 2) * (arrivalPeriodMillisReal) / (1000 * tasksPerJob));

                    LOG.debug("APReal: " + arrivalPeriodMillisReal + "APFake: " + arrivalPeriodMillisFake + "; ARReal: " +
                            arrivalRateReal + "ARFake: " + arrivalRateFake + "; TD: " + taskDurationMillis + "; SR: " + serviceRate +
                            "; TOTAL TASK NUMBER: " + totalNoOfTasks + ", Extra Tasks: " + EXTRA_TASKS);

                    LOG.debug("Using Real arrival period of " + arrivalPeriodMillisReal +
                            " milliseconds and running experiment for " + experimentDurationS + " seconds.");
                    JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurations, workSpeedMap.toString());
                    JobLaunchRunnable runnableFake = new JobLaunchRunnable(tasksPerJob, taskDurationsFake, workSpeedMap.toString());
                    taskLauncher = new ScheduledThreadPoolExecutor(1);
                    runnableFake.setFake(true);
                    ScheduledFuture<?> sf = taskLauncher.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillisReal, TimeUnit.MILLISECONDS);
                    ScheduledFuture<?> sfFake = taskLauncher.scheduleAtFixedRate(runnableFake, 0, arrivalPeriodMillisFake, TimeUnit.MILLISECONDS);

                } else {
                    load = realLoad;
                    LOG.debug("Disabling Fake Tasks");
                    double arrivalRate = load * serviceRate;
                    //Get Arrival Period for individual task
                    arrivalPeriodMillis = (long) (tasksPerJob / arrivalRate);

                    //Get Experiment duration based on no. of tasks (in s)
                    //Need to add more seconds to make sure all tasks get executed
                    experimentDurationS = (int) ((totalNoOfTasks) * (arrivalPeriodMillis) / (1000 * tasksPerJob));

                    LOG.debug("AP: " + arrivalPeriodMillis + "; AR: " + arrivalRate + "; TD: " + taskDurationMillis + "; SR: " + serviceRate +
                            "; TOTAL TASK NUMBER: " + totalNoOfTasks + ", Extra Tasks: " + EXTRA_TASKS);

                    LOG.debug("Using arrival period of " + arrivalPeriodMillis +
                            " milliseconds and running experiment for " + experimentDurationS + " seconds.");
                    //Passing workerSpeedMap as string from frontend to scheduler
                    JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurations, workSpeedMap.toString());
                    taskLauncher = new ScheduledThreadPoolExecutor(1);
                    taskLauncher.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillis, TimeUnit.MILLISECONDS);
                }
                long startTime = System.currentTimeMillis();
                LOG.debug("sleeping");
                while (System.currentTimeMillis() < startTime + experimentDurationS * 1000) {
                    Thread.sleep(100);
                }
                taskLauncher.shutdown();

            } else {
                experimentDurationS = 11 * 60; //RUnning for 11 minutes
                LOG.debug(realLoads + "<------");
                if (realLoads.equalsIgnoreCase("") || fakeLoads.equalsIgnoreCase("")) {
                    LOG.debug("Warning!!! Empty alteration");
                } else {
                    //Create Hashmap from the string
                    //Will use this function in the util because this is being used everywhere.
                    String[] keyValuePairs = realLoads.split(",");              //split the string to creat key-value pairs
                    int counter = 0;
                    for (String pair : keyValuePairs)                        //iterate over the pairs
                    {
                        mapRealLoad.put((int) counter / 60, Double.valueOf(pair));
                        counter = counter + arrivalConfigTime;
                    }
                    LOG.debug("Real Load: " + mapRealLoad.toString());
                    String[] keyValuePairsFake = fakeLoads.split(",");              //split the string to creat key-value pairs
                    int counter1 = 0;
                    for (String pair : keyValuePairsFake)                        //iterate over the pairs
                    {
                        mapFakeLoad.put((int) counter / 60, Double.valueOf(pair));
                        counter = counter + arrivalConfigTime;
                    }
                }

                LOG.debug("sleeping");
                long startTime = System.currentTimeMillis();
                int minuteCounter = (int) (((System.currentTimeMillis() / 1000) / 60) % 60); //Is there a chance that
                JobLaunchRunnable runnable = new JobLaunchRunnable(tasksPerJob, taskDurations, workSpeedMap.toString());
                JobLaunchRunnable runnableFake = new JobLaunchRunnable(tasksPerJob, taskDurationsFake, workSpeedMap.toString());
                taskLauncher = new ScheduledThreadPoolExecutor(1);

                ScheduledFuture<?> sf = null;
                ScheduledFuture<?> sfFake = null;
                while (System.currentTimeMillis() < startTime + experimentDurationS * 1000) {
                    Thread.sleep(100);
                    if (fakeTasks == 1) {
                        int minutes = (int) (((System.currentTimeMillis() / 1000) / 60) % 60);

                        if (mapRealLoad.get(minutes) != null && mapFakeLoad.get(minutes) != null) { //Currently both fakeload and real load change at the same time
                            if (minuteCounter <= minutes) {
                                if (sf != null && sfFake != null) {
                                    sf.cancel(true);
                                    sfFake.cancel(true);
                                }

                                double arrivalRateReal = mapRealLoad.get(minutes) * serviceRate;
                                double arrivalRateFake = mapFakeLoad.get(minutes) * serviceRate;
                                arrivalPeriodMillisReal = (long) (tasksPerJob / arrivalRateReal);
                                arrivalPeriodMillisFake = (long) (tasksPerJob / arrivalRateFake);
                                runnableFake.setFake(true);
                                LOG.debug("New Real Arrival Period------------> " + arrivalPeriodMillisReal);
                                LOG.debug("New Fake Arrival Period------------> " + arrivalPeriodMillisFake);

                                sf = taskLauncher.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillisReal, TimeUnit.MILLISECONDS);
                                sfFake = taskLauncher.scheduleAtFixedRate(runnableFake, 0, arrivalPeriodMillisFake, TimeUnit.MILLISECONDS);
                                minuteCounter = minuteCounter + (int) arrivalConfigTime / 60;
                            }
                        }
                    } else {
                        int minutes = (int) (((System.currentTimeMillis() / 1000) / 60) % 60);
                        if (mapRealLoad.get(minutes) != null) {
                            if (minuteCounter <= minutes) {
                                if (sf != null) {
                                    sf.cancel(true);
                                }
                                load = mapRealLoad.get(minutes);
                                double arrivalRate = load * serviceRate;
                                arrivalPeriodMillis = (long) (tasksPerJob / arrivalRate);

                                LOG.debug("New Arrival Period------------> " + arrivalPeriodMillis);
                                sf = taskLauncher.scheduleAtFixedRate(runnable, 0, arrivalPeriodMillis, TimeUnit.MILLISECONDS);
                                minuteCounter = minuteCounter + (int) arrivalConfigTime / 60;
                            }
                        }

                    }

                }
                if (sf != null) {
                    sf.cancel(true);
                }
            }


        } catch (Exception e) {
            LOG.error("Fatal exception", e);
        }
    }

    @Override
    public void frontendMessage(TFullTaskId taskId, int status, ByteBuffer message)
            throws TException {
        // We don't use messages here, so just log it.
        LOG.debug("Got unexpected message: " + Serialization.getByteBufferContents(message));
    }

    public static void main(String[] args) {
        new SimpleFrontend().run(args);

    }
}
