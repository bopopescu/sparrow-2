package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import edu.berkeley.sparrow.daemon.util.*;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;
import org.w3c.dom.NodeList;

/**
 * A task placer which probes node monitors in order to determine placement.
 */
public class ProbingTaskPlacer implements TaskPlacer {
    private static final Logger LOG = Logger.getLogger(ProbingTaskPlacer.class);
    protected static final Logger AUDIT_LOG = Logging.getAuditLogger(ProbingTaskPlacer.class);

    private static final int MAX_PROBE_WAIT_MS = 5;

    /**
     * See {@link SparrowConf.PROBE_MULTIPLIER}
     */
    private double probeRatio;
    private int policy;
    private int scaled;
    private String fakeLoadRatio;
    double realLoad;
    double fakeLoad;
    int isHalo;
    private ThriftClientPool<AsyncClient> clientPool;
    private RandomTaskPlacer randomPlacer;
    private final String DEFAULT_WORKER_SPEED = "1";
    double multiArm;


    /**
     * This acts as a callback for the asynchronous Thrift interface.
     */
    protected class ProbeCallback implements AsyncMethodCallback<getLoad_call> {
        InetSocketAddress socket;
        /**
         * This should not be modified after the {@code latch} count is zero!
         */
        Map<InetSocketAddress, TResourceUsage> loads;
        /**
         * Synchronization latch so caller can return when enough backends have
         * responded.
         */
        CountDownLatch latch;
        private String appId;
        private String requestId;
        private AsyncClient client;
        private double chosenWorkerSpeed;

        ProbeCallback(
                InetSocketAddress socket, Map<InetSocketAddress, TResourceUsage> loads,
                CountDownLatch latch, String appId, String requestId, AsyncClient client, Double chosenWorkerSpeed) {
            this.socket = socket;
            this.loads = loads;
            this.latch = latch;
            this.appId = appId;
            this.requestId = requestId;
            this.client = client;
            this.chosenWorkerSpeed = chosenWorkerSpeed;
        }

        @Override
        public void onComplete(getLoad_call response) {
            try {
                LOG.debug("Received load response from node " + socket + ": load : " + response.getResult().toString());
            } catch (TException e1) {
                LOG.error("Probe returned no information for " + appId);
            }
            // TODO: Include the port, as well as the address, in the log message, so this
            // works properly when multiple daemons are running on the same machine.
            int queueLength = -1;
            int fakeQueueLength = -1;
            int cores = -1;
            try {
                queueLength = response.getResult().get(appId).queueLength;
                fakeQueueLength = response.getResult().get(appId).fakeQueueLength;
                cores = response.getResult().get(appId).resources.cores;
            } catch (TException e1) {
                LOG.error("Probe returned no information for " + appId);
            }

            AUDIT_LOG.info(Logging.auditEventString("probe_completion", requestId,
                    socket.getAddress().getHostAddress(),
                    queueLength, fakeQueueLength, cores));
            try {
                Map<String, TResourceUsage> resp = response.getResult();
                if (!resp.containsKey(appId)) {
                    LOG.warn("Probe returned no load information for " + appId);
                } else {
                    TResourceUsage result = response.getResult().get(appId);
                    result.setWorkSpeed(chosenWorkerSpeed);
                    loads.put(socket, result);
                    latch.countDown();
                }
                clientPool.returnClient(socket, client);
            } catch (Exception e) {
                LOG.error("Error getting resources from response data", e);
            }
        }

        @Override
        public void onError(Exception exception) {
            LOG.error("Error in probe callback", exception);
            // TODO: Figure out what failure model we want here
            latch.countDown();
        }
    }

    @Override
    public void initialize(Configuration conf, ThriftClientPool<AsyncClient> clientPool) {
        probeRatio = conf.getDouble(SparrowConf.SAMPLE_RATIO,
                SparrowConf.DEFAULT_SAMPLE_RATIO);
        policy = conf.getInt(SparrowConf.POLICY, SparrowConf.DEFAULT_POLICY);
        scaled = conf.getInt(SparrowConf.SCALED, SparrowConf.DEFAULT_SCALED);
        isHalo = conf.getInt(SparrowConf.HALO, SparrowConf.DEFAULT_HALO);
        fakeLoadRatio = conf.getString(SparrowConf.FAKE_LOAD_RATIO, SparrowConf.DEFAULT_FAKE_LOAD_RATIO);
        multiArm = conf.getDouble(SparrowConf.MULTIARM, SparrowConf.DEFAULT_MULTIARM);
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

        this.clientPool = clientPool;
        randomPlacer = new RandomTaskPlacer();
        randomPlacer.initialize(conf, clientPool);
    }

    private List<InetSocketAddress> returnNodeList(int probesToLaunch, double[] cdf_worker_speed, HashMap<String, InetSocketAddress> nodeToInetMap, ArrayList<String> backendList, int isHalo) {
        //This was used to make sure probes aren't sent to same worker
        ArrayList<Integer> workerIndex = new ArrayList<Integer>();
        //This is supposed to be the nodelist for specified no. of probes
        List<InetSocketAddress> subNodeList = new ArrayList<InetSocketAddress>();

        for (int i = 0; i < probesToLaunch; i++) {
            if (isHalo == 1) {
                LOG.debug("Halo!!");
                //ConfigFunctions.getCDFWorkerSpeedHalo()
                int workerIndexReservation = ConfigFunctions.getIndexHalo(cdf_worker_speed, workerIndex);
                workerIndex.add(workerIndexReservation); //Chosen workers based on proportional sampling
            } else {
                int workerIndexReservation = ConfigFunctions.getIndexFromPSS(cdf_worker_speed, workerIndex);
                workerIndex.add(workerIndexReservation); //Chosen workers based on proportional sampling
            }
        }

        //After ConfigFunctions, we're getting the index of worker with higher probability
        //Nodelist contains the list of workers and workerIndex contains indices from that node list
        //So this comparision should make sense but using hashmap would be a better idea.
        for (int j = 0; j < workerIndex.size(); j++) {
            String hostFromWorkerSpeed = backendList.get(workerIndex.get(j));
            subNodeList.add(nodeToInetMap.get(hostFromWorkerSpeed));
        }
        return subNodeList;
    }

    @Override
    public Collection<TaskPlacer.TaskPlacementResponse> placeTasks(String appId,
                                                                   String requestId, Collection<InetSocketAddress> nodes,
                                                                   Collection<TTaskSpec> tasks, HashMap<String, Double> workerSpeedMap)
            throws IOException {
//        LOG.debug(Logging.functionCall(appId, nodes, tasks));

        if (probeRatio < 1.0) {
            LOG.debug("Random Enabled");
            return randomPlacer.placeTasks(appId, requestId, nodes, tasks, workerSpeedMap);
        }

        Map<InetSocketAddress, TResourceUsage> loads = Maps.newConcurrentMap();

        // This latch decides how many nodes need to respond for us to make a decision.
        // Using a simple counter is okay for now, but eventually we will want to use
        // per-task information to decide when to return.
        //TODO This assumes that we have more workers than probes
        int probesToLaunch = (int) Math.ceil(probeRatio * tasks.size());



        // Right now we wait for all probes to return, in the future we might add a timeout
        CountDownLatch latch = new CountDownLatch(probesToLaunch);

        ArrayList<String> backendList = new ArrayList<String>();
        ArrayList<Double> workerSpeedList = new ArrayList<Double>();
        //Scheculer sends the workerspeed map if estimated then this might be empty
        //Probing task placer doesn't care whether learning is used or not
        //Scheduler handles the logic

        for (Map.Entry<String, Double> entry : workerSpeedMap.entrySet()) {
            backendList.add(entry.getKey());
            workerSpeedList.add(entry.getValue());
        }

        //All the currently available nodes
        List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
        //So that the node Object can be loaded later
        HashMap<String, InetSocketAddress> nodeToInetMap = new HashMap<String, InetSocketAddress>();
        for (InetSocketAddress node : nodeList) {
            nodeToInetMap.put(node.getAddress().getHostAddress(), node);
        }
        double[] cdf_worker_speed = null;
        try {
            //gets cdf of worker speed in the range of 0 to 1
            //Have to be careful about the index
            if (isHalo == 1) {
                cdf_worker_speed = ConfigFunctions.getCDFWorkerSpeedHalo(workerSpeedList, realLoad); //TODO
            } else {
                cdf_worker_speed = ConfigFunctions.getCDFWokerSpeed(workerSpeedList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        if (policy == 1) { //PSS + POT where probes to launch determines
            if (nodes.size() > probesToLaunch) {
                LOG.debug("Launching " + probesToLaunch + " probes");
                nodeList = returnNodeList(probesToLaunch, cdf_worker_speed, nodeToInetMap, backendList, isHalo);
            } else {
                LOG.debug("WARNING :: No. of Probes is greater than Nodes Size. PSS not running. Only sending specified no. of probes.");
                LOG.debug("Currently Probing all available machines");
                // Get a random subset of nodes by shuffling list . This can be used if we're not using pss
                Collections.shuffle(nodeList);
                nodeList = nodeList.subList(0, nodeList.size());
            }
        } else if (policy == 2) { //PSS i.e probes all the machines
            LOG.debug("Only PSS");
            nodeList = returnNodeList(1, cdf_worker_speed, nodeToInetMap, backendList, isHalo);
        } else if (policy == 3) {
            LOG.debug("POT Without PSS");
            if (nodeList.size() > 1) {
                Collections.shuffle(nodeList);
                nodeList = nodeList.subList(0, 2); //Hardcode 2 from Power of Two here
            }
        } else if (policy == 4) {
            double armSeed = Math.random();
            if (armSeed < multiArm) { //With Probability Delta Random is chosen
                LOG.debug("Arm goto Random");
                if (nodeList.size() > 1) {
                    return randomPlacer.placeTasks(appId, requestId, nodes, tasks, workerSpeedMap);
             //       Collections.shuffle(nodeList);
               //     nodeList = nodeList.subList(0, 1); //uniform
                }
            } else {  //With Probability 1-Delta PSS+POT is chosen
                LOG.debug("Arm goto PSS+POT");
                if (nodes.size() > probesToLaunch) {
                    LOG.debug("Real PSS+POT");
                    nodeList = returnNodeList(probesToLaunch, cdf_worker_speed, nodeToInetMap, backendList, isHalo);
                } else {
                    LOG.debug("Arm WARNING :: No. of Probes is greater than Nodes Size. PSS not running. Only sending specified no. of probes.");
                    LOG.debug("Currently Probing all available machines");
                    // Get a random subset of nodes by shuffling list . This can be used if we're not using pss
                    //This shouldn't be used but is here just in case
                    Collections.shuffle(nodeList);
                    nodeList = nodeList.subList(0, nodeList.size());
                }

            }

        }

        ArrayList<Double> chosenWorkerSpeedsByPSS = new ArrayList<Double>();
        for (InetSocketAddress nodeWorkerSpeed : nodeList) {
            //From the backend List get the node that matches the address from NodeList
            int i = backendList.indexOf(nodeWorkerSpeed.getAddress().getHostAddress());
            //Get the workerSpeed with the same index(since that's how we created backend list and workersspeed list)
            //Chosen workerspeed List to be used for scaled implementation
            chosenWorkerSpeedsByPSS.add(workerSpeedList.get(i));
        }

        //all the nodes will be probed if there are more less machine than the probes
        for (InetSocketAddress node : nodeList) {
            try {
                AsyncClient client = clientPool.borrowClient(node);

                //From the backend List get the node that matches the address from NodeList
                int i = backendList.indexOf(node.getAddress().getHostAddress());
                //Get the workerSpeed with the same index(since that's how we created backend list and workersspeed list)
                //Chosen workerspeed List to be used for scaled implementation
                Double currentWorkerSpeed = workerSpeedList.get(i);

                ProbeCallback callback = new ProbeCallback(node, loads, latch, appId, requestId,
                        client, currentWorkerSpeed);
                LOG.debug("Launching probe on node: " + node);
                AUDIT_LOG.info(Logging.auditEventString("probe_launch", requestId,
                        node.getAddress().getHostAddress()));
                client.getLoad(appId, requestId, callback);
            } catch (Exception e) {
                LOG.error(e);
            }
        }


        try {
            latch.await(MAX_PROBE_WAIT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        for (InetSocketAddress machine : nodeList) {
            if (!loads.containsKey(machine)) {
                // TODO maybe use stale data here?
                LOG.debug("Load doesn't contain Machine address" + machine);
                // Assume this machine is really heavily loaded
                loads.put(machine,
                        TResources.createResourceUsage(
                                TResources.createResourceVector(1000, 4), 100));
            }
        }
        Collection<TaskPlacementResponse> out;
        if (scaled == 1) {
            LOG.debug("Running Scaled version");
            AssignmentPolicy assigner = new ComparatorAssignmentPolicy(
                    new TResources.ScaledQueueComparator());
            out = assigner.assignTasks(tasks, loads);
        } else {
            LOG.debug("Running UnScaled version");
            AssignmentPolicy assigner = new ComparatorAssignmentPolicy(
                    new TResources.MinQueueComparator());
            out = assigner.assignTasks(tasks, loads);
        }

        return out;
    }
}
