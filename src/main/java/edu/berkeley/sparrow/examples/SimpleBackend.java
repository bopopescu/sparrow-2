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

import com.google.common.collect.Lists;
import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.nodemonitor.NodeMonitorThrift;
import edu.berkeley.sparrow.daemon.util.TClients;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.BackendService;
import edu.berkeley.sparrow.thrift.NodeMonitorService;
import edu.berkeley.sparrow.thrift.NodeMonitorService.Client;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TUserGroupInfo;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.StringReader;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.transport.TTransportException;

/**
 * A prototype Sparrow backend that runs sleep tasks.
 */
public class SimpleBackend implements BackendService.Iface {

    private static final String LISTEN_PORT = "listen_port";
    private static final int DEFAULT_LISTEN_PORT = 20101;

    public static AtomicInteger numTasks = new AtomicInteger(0);
    public static long startTime = -1;

    /**
     * Each task is launched in its own thread from a thread pool with WORKER_THREADS threads,
     * so this should be set equal to the maximum number of tasks that can be running on a worker.
     */
    private static final int THRIFT_WORKER_THREADS = 1;
    private static final int WORKER_THREADS = 1;
    private static final String APP_ID = "sleepApp";

    /**
     * Configuration parameters to specify where the node monitor is running.
     */
    private static final String NODE_MONITOR_HOST = "node_monitor_host";
    private static final String DEFAULT_NODE_MONITOR_HOST = "localhost";
    private static String NODE_MONITOR_PORT = "node_monitor_port";
    private static int nodeMonitorPort;
    private static int totalNoOfTasks=100;
    private static String nodeMonitorHost = "localhost";

    private static String SLAVES = "slaves";
    private static String DEFAULT_NO_SLAVES = "slaves";

    private static Double globalHostWorkerSpeed = -1.0;

    /**
     * Total Number of tasks.
     */
    public static final String TOTAL_NO_OF_TASKS = "total_no_tasks";
    public static final int DEFAULT_TOTAL_NO_OF_TASKS = 3500;

    private static Client client;
    private static String workSpeed;

    private static final Logger LOG = Logger.getLogger(SimpleBackend.class);
    private static final ExecutorService executor =
            Executors.newFixedThreadPool(WORKER_THREADS);

    /**
     * Keeps track of finished tasks.
     * <p>
     * A single thread pulls items off of this queue and uses
     * the client to notify the node monitor that tasks have finished.
     */
    private final BlockingQueue<TFullTaskId> finishedTasks = new LinkedBlockingQueue<TFullTaskId>();

    /**
     * Thread that sends taskFinished() RPCs to the node monitor.
     * <p>
     * We do this in a single thread so that we just need a single client to the node monitor
     * and don't need to create a new client for each task.
     */
    private class TasksFinishedRpcRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    TFullTaskId task = finishedTasks.take();
                    client.tasksFinished(Lists.newArrayList(task));
                } catch (InterruptedException e) {
                    LOG.error("Error taking a task from the queue: " + e.getMessage());
                } catch (TException e) {
                    LOG.error("Error with tasksFinished() RPC:" + e.getMessage());
                }
            }
        }
    }

    /**
     * Thread spawned for each task. It runs for a given amount of time (and adds
     * its resources to the total resources for that time) then stops. It updates
     * the NodeMonitor when it launches and again when it finishes.
     */
    private class TaskRunnable implements Runnable {
        private double taskDuration;
        private TFullTaskId taskId;
        private long taskStartTime;

        public TaskRunnable(String requestId, TFullTaskId taskId, ByteBuffer message) {
            this.taskStartTime = message.getLong();
            this.taskDuration = message.getDouble();
            this.taskId = taskId;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            NodeMonitorService.Client client = null;
            try {
                client = TClients.createBlockingNmClient(nodeMonitorHost, nodeMonitorPort);
            } catch (IOException e) {
                LOG.fatal("Error creating NM client", e);
            }

            Double hostWorkSpeed = -1.0; //Initialization. The value will be replaced

            String thisHost = null;
            try {
                thisHost = Inet4Address.getLocalHost().getHostAddress();

                //Getting rid of repeated parsing of the string
                if (globalHostWorkerSpeed == -1.0) {
                    Properties props = new Properties();
                    props.load(new StringReader(workSpeed.replace(",", "\n")));
                    for (Map.Entry<Object, Object> e : props.entrySet()) {
                        if ((String.valueOf(e.getKey())).equals(thisHost)) {
                            hostWorkSpeed = Double.valueOf((String) e.getValue());
                        }
                    }
                    globalHostWorkerSpeed = hostWorkSpeed;
                } else {
                    hostWorkSpeed = globalHostWorkerSpeed;
                }

                long sleepTime = (long) ((Double.valueOf(taskDuration) / Double.valueOf(hostWorkSpeed)));

                Thread.sleep(sleepTime);

                LOG.debug("WS: " + hostWorkSpeed + "ms" + ";  Host: " + thisHost + "; sleepTime: " + sleepTime + "; taskDuration " + taskDuration);

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

//            int tasks = numTasks.addAndGet(1);
//            double taskRate = ((double) tasks) * 1000 /
//                    (System.currentTimeMillis() - startTime);

//          LOG.debug("Aggregate task rate: " + taskRate);

            LOG.debug("Actual task in " + (taskDuration) + "ms");
            LOG.debug("Task completed in " + (System.currentTimeMillis() - startTime) + "ms");
            LOG.debug("ResponseTime in " + (System.currentTimeMillis() - taskStartTime) + "ms");
            LOG.debug("WaitingTime in " + (startTime - taskStartTime) + "ms");

            try {
                client.tasksFinished(Lists.newArrayList(taskId));
            } catch (TException e) {
                e.printStackTrace();
            }


            try {
                client.tasksFinished(Lists.newArrayList(taskId));
                String requestId = taskId.getRequestId();
                if ((requestId.substring(requestId.lastIndexOf("_") + 1)).equalsIgnoreCase(String.valueOf(totalNoOfTasks))){
                    List<String> lines = Arrays.asList("Just finished Launching request"+ requestId);
                    try {
                        Path file = Paths.get("/root/finishedLaunchingFinalTask.comp");
                        Files.write(file, lines, Charset.forName("UTF-8"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    ByteBuffer message = ByteBuffer.allocate(8);
                    //Send the task Completion Time
                    message.putDouble(Double.valueOf(11111)); //Just for check Think about what we really want to pass
                    client.sendFrontendMessage(taskId.appId,taskId,1, ByteBuffer.wrap(message.array()));
                }

            } catch (TException e) {
                e.printStackTrace();
            }
            //TODO Understand why this is being done.
            client.getInputProtocol().getTransport().close();
            client.getOutputProtocol().getTransport().close();
        }
    }

    /**
     * Initializes the backend by registering with the node monitor.
     * <p>
     * Also starts a thread that handles finished tasks (by sending an RPC to the node monitor).
     */
    public void initialize(int listenPort, String nodeMonitorHost, int nodeMonitorPort) {
        // Register server.
        try {
            client = TClients.createBlockingNmClient(nodeMonitorHost, nodeMonitorPort);
        } catch (IOException e) {
            LOG.debug("Error creating Thrift client: " + e.getMessage());
        }

        try {
            client.registerBackend(APP_ID, "localhost:" + listenPort);
            LOG.debug("Client successfully registered");
        } catch (TException e) {
            LOG.debug("Error while registering backend: " + e.getMessage());
        }

        new Thread(new TasksFinishedRpcRunnable()).start();
    }

    @Override
    public void launchTask(ByteBuffer message, TFullTaskId taskId,
                           TUserGroupInfo user) throws TException {
        //LOG.info("Submitting task " + taskId.getTaskId() + " at " + System.currentTimeMillis());

        executor.submit(new TaskRunnable(
                taskId.requestId, taskId, message));
    }

    public static void main(String[] args) throws IOException, TException {
        OptionParser parser = new OptionParser();
        parser.accepts("c", "configuration file").
                withRequiredArg().ofType(String.class);
        parser.accepts("w", "configuration file").
                withRequiredArg().ofType(String.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(-1);
        }

        // Logger configuration: log to the console
        BasicConfigurator.configure();
        LOG.setLevel(Level.DEBUG);
        LOG.debug("debug logging on");


        Configuration conf = new PropertiesConfiguration();


        if (options.has("c")) {
            String configFile = (String) options.valueOf("c");
            try {
                conf = new PropertiesConfiguration(configFile);
            } catch (ConfigurationException e) {
            }
        }

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
        workSpeed = "";
        for (String node : slavesConfig.getStringArray(SLAVES)) {
            workSpeed = workSpeed + node + ",";
        }


        // Start backend server
        BackendService.Processor<BackendService.Iface> processor =
                new BackendService.Processor<BackendService.Iface>(new SimpleBackend());

        int listenPort = conf.getInt(LISTEN_PORT, DEFAULT_LISTEN_PORT);
        TServers.launchThreadedThriftServer(listenPort, THRIFT_WORKER_THREADS, processor);

        nodeMonitorPort = conf.getInt(NODE_MONITOR_PORT, NodeMonitorThrift.DEFAULT_NM_THRIFT_PORT);
        nodeMonitorHost = conf.getString(NODE_MONITOR_HOST, DEFAULT_NODE_MONITOR_HOST);
        totalNoOfTasks= conf.getInt(TOTAL_NO_OF_TASKS, DEFAULT_TOTAL_NO_OF_TASKS);

        // Register server
        client = TClients.createBlockingNmClient(nodeMonitorHost, nodeMonitorPort);

        try {
            client.registerBackend(APP_ID, "localhost:" + listenPort);
            LOG.debug("Client successfullly registered");
        } catch (TTransportException e) {
            LOG.debug("Error while registering backend: " + e.getMessage());
        }

    }
}