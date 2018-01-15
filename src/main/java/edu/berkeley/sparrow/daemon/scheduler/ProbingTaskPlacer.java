package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import edu.berkeley.sparrow.daemon.util.ConfVariable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient.getLoad_call;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

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

    private ThriftClientPool<AsyncClient> clientPool;
    private RandomTaskPlacer randomPlacer;


    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    //Gets index  where cdf allows retrieving index having higher workerspeed with higher probability
    public static int getIndexFromPSS(double[] cdf_worker_speed, ArrayList<Integer> workerIndex) {
        UniformRealDistribution uniformRealDistribution = new UniformRealDistribution();
        int workerIndexReservation = java.util.Arrays.binarySearch(cdf_worker_speed, uniformRealDistribution.sample());
        if (workerIndexReservation < 0) {
            workerIndexReservation = Math.abs(workerIndexReservation) - 1;
        } else {
            workerIndexReservation = Math.abs(workerIndexReservation);
        }
        //This doesn't allow probing the same nodemonitor twice
        if (workerIndex.contains(workerIndexReservation)) {
            workerIndexReservation = getIndexFromPSS(cdf_worker_speed, workerIndex);
        }
        return workerIndexReservation;
    }


    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    public static double[] getCDFWokerSpeed(ArrayList<Double> workerSpeedList) throws IOException {

        //Gets the CDF of workers Speed
        double sum = 0;
        for (double d : workerSpeedList)
            sum += d;

        double[] cdf_worker_speed = new double[workerSpeedList.size()];
        double cdf = 0;
        int j = 0;
        for (double d : workerSpeedList) {
            d = d / sum;
            cdf = cdf + d;
            cdf_worker_speed[j] = cdf;
            j++;
        }
        //CDF of worker speed + PSS based on Qiong's python pss file
        return cdf_worker_speed;
    }

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

        ProbeCallback(
                InetSocketAddress socket, Map<InetSocketAddress, TResourceUsage> loads,
                CountDownLatch latch, String appId, String requestId, AsyncClient client) {
            this.socket = socket;
            this.loads = loads;
            this.latch = latch;
            this.appId = appId;
            this.requestId = requestId;
            this.client = client;
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
            int cores = -1;
            try {
                queueLength = response.getResult().get(appId).queueLength;
                cores = response.getResult().get(appId).resources.cores;
            } catch (TException e1) {
                LOG.error("Probe returned no information for " + appId);
            }
            AUDIT_LOG.info(Logging.auditEventString("probe_completion", requestId,
                    socket.getAddress().getHostAddress(),
                    queueLength, cores));
            try {
                Map<String, TResourceUsage> resp = response.getResult();
                if (!resp.containsKey(appId)) {
                    LOG.warn("Probe returned no load information for " + appId);
                } else {
                    TResourceUsage result = response.getResult().get(appId);
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
        this.clientPool = clientPool;
        randomPlacer = new RandomTaskPlacer();
        randomPlacer.initialize(conf, clientPool);
    }

    @Override
    public Collection<TaskPlacer.TaskPlacementResponse> placeTasks(String appId,
                                                                   String requestId, Collection<InetSocketAddress> nodes,
                                                                   Collection<TTaskSpec> tasks, String workerSpeedMap, HashMap<String, String> estimatedWorkerSpeedMap)
            throws IOException {
        LOG.debug(Logging.functionCall(appId, nodes, tasks));
        LOG.debug("Real: " + workerSpeedMap + "VS: Estimated:  " + estimatedWorkerSpeedMap);

        if (probeRatio < 1.0) {
            return randomPlacer.placeTasks(appId, requestId, nodes, tasks, workerSpeedMap, estimatedWorkerSpeedMap);
        }

        Map<InetSocketAddress, TResourceUsage> loads = Maps.newConcurrentMap();

        // This latch decides how many nodes need to respond for us to make a decision.
        // Using a simple counter is okay for now, but eventually we will want to use
        // per-task information to decide when to return.
        //TODO This assumes that we have more workers than probes
        int probesToLaunch = SparrowConf.DEFAULT_SAMPLE_RATIO_CONSTRAINED;//Math.min(probesToLaunch, nodes.size());
        LOG.debug("Launching " + probesToLaunch + " probes");
        // Right now we wait for all probes to return, in the future we might add a timeout
        CountDownLatch latch = new CountDownLatch(probesToLaunch);
        //All the available nodes
        List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
        //We need to pass the arrayList to calculate PSS
        ArrayList<Double> workerSpeedList = new ArrayList<Double>();
        ArrayList<String> backendList = new ArrayList<String>();
        //This is supposed to be the nodelist for specified no. of probes
        List<InetSocketAddress> subNodeList = new ArrayList<InetSocketAddress>();
        HashMap<InetSocketAddress, String> nodeListMap = new HashMap<InetSocketAddress, String>();



        //NodeList Map is sorted based on nodeList and it has worker speed from  estimatedWorker Speed
        for (InetSocketAddress node : nodeList) {
            if (estimatedWorkerSpeedMap.isEmpty()) {
                nodeListMap.put(node, "1");
            } else {
                nodeListMap.put(node, estimatedWorkerSpeedMap.get(node.getAddress().getHostAddress()));
            }
        }

        for (Map.Entry<InetSocketAddress, String> entry : nodeListMap.entrySet()) {
            backendList.add(entry.getKey().getAddress().getHostAddress().toString());
            workerSpeedList.add(Double.valueOf(entry.getValue().toString()));
        }

        LOG.debug("Matches Indices of Estimated Vs NodeList" + estimatedWorkerSpeedMap.toString() + ":" + nodeListMap.toString());

        double[] cdf_worker_speed = null;

        try {
            //gets cdf of worker speed in the range of 0 to 1
            cdf_worker_speed = getCDFWokerSpeed(workerSpeedList);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //This was used to make sure probes aren't sent to same worker
        ArrayList<Integer> workerIndex = new ArrayList<Integer>();
        if (nodes.size() > probesToLaunch) {
            for (int i = 0; i < probesToLaunch; i++) {
                int workerIndexReservation = getIndexFromPSS(cdf_worker_speed, workerIndex);
                workerIndex.add(workerIndexReservation); //Chosen workers based on proportional sampling
            }

            //After PSS, we're getting the index of worker with higher probability
            //Nodelist contains the list of workers and workerIndex contains indices from that node list
            //So this comparision should make sense but using hashmap would be a better idea.
            for (int j = 0; j < workerIndex.size(); j++) {
                subNodeList.add(nodeList.get(workerIndex.get(j)));
            }
            nodeList = subNodeList;
        }

        for (InetSocketAddress node : nodeList) {
            try {
                AsyncClient client = clientPool.borrowClient(node);
                ProbeCallback callback = new ProbeCallback(node, loads, latch, appId, requestId,
                        client);
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
        AssignmentPolicy assigner = new ComparatorAssignmentPolicy(
                new TResources.MinQueueComparator());
        Collection<TaskPlacementResponse> out = assigner.assignTasks(tasks, loads);

        return out;
    }
}
