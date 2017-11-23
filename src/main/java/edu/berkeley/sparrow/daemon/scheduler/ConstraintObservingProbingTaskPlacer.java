package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Logging;
import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

public class  ConstraintObservingProbingTaskPlacer extends ProbingTaskPlacer {
  public static int MAXIMUM_PROBE_WAIT_MS = 5; // Longest time we wait to sample

  public int probesPerTask;

  private final static Logger LOG =
      Logger.getLogger(ConstraintObservingProbingTaskPlacer.class);

  private ThriftClientPool<AsyncClient> clientPool;
  private AssignmentPolicy waterLevelPolicy = new ConstrainedTaskAssignmentPolicy(
      new WaterLevelAssignmentPolicy());
  private AssignmentPolicy randomPolicy = new ConstrainedTaskAssignmentPolicy(
      new RandomAssignmentPolicy());

  @Override
  public void initialize(Configuration conf,
      ThriftClientPool<AsyncClient> clientPool) {
    this.clientPool = clientPool;
    probesPerTask = conf.getInt(SparrowConf.SAMPLE_RATIO_CONSTRAINED,
        SparrowConf.DEFAULT_SAMPLE_RATIO_CONSTRAINED);
    super.initialize(conf, clientPool);
  }

  @Override
  public Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes,
      Collection<TTaskSpec> tasks) throws IOException {
    LOG.debug("Placing constrained tasks with probe ratio: " + probesPerTask);

    // This approximates a "randomized over constraints" approach if we get a trivial
    // probe ratio.
    if (probesPerTask < 1.0) {
      Set<TaskPlacementResponse> responses = Sets.newHashSet();
      for (TTaskSpec task: tasks) {
        List<TTaskSpec> taskList = Lists.newArrayList(task);
        // Should return three neighbors
        Collection<InetSocketAddress> machinesToProbe = getMachinesToProbe(
            nodes, taskList, 3);

        // Resource info is ignored by random policy
        Map<InetSocketAddress, TResourceUsage> mockedResources = Maps.newHashMap();
        for (InetSocketAddress socket : machinesToProbe) {
          TResourceUsage usage = new TResourceUsage();
          // Resource info is ignored by random policy
          usage.queueLength = 0;
          usage.resources = TResources.createResourceVector(0, 0);
          mockedResources.put(socket, usage);
        }
        responses.addAll(randomPolicy.assignTasks(taskList, mockedResources));
      }
      return responses;
    }

    Collection<InetSocketAddress> machinesToProbe = getMachinesToProbe(nodes, tasks,
        probesPerTask);
    CountDownLatch latch = new CountDownLatch(machinesToProbe.size());
    Map<InetSocketAddress, TResourceUsage> loads = Maps.newConcurrentMap();
    for (InetSocketAddress machine: machinesToProbe) {
      AsyncClient client = null;
      try {
        client = clientPool.borrowClient(machine);
      } catch (Exception e) {
        LOG.fatal(e);
      }
      try {
        AUDIT_LOG.info(Logging.auditEventString("probe_launch", requestId,
            machine.getAddress().getHostAddress()));
        client.getLoad(appId, requestId,
            new ProbeCallback(machine, loads, latch, appId, requestId, client));
      } catch (TException e) {
        LOG.fatal(e);
      }
    }
    try {
      latch.await(MAXIMUM_PROBE_WAIT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (InetSocketAddress machine : machinesToProbe) {
      if (!loads.containsKey(machine)) {
        // TODO maybe use stale data here?
        // Assume this machine is really heavily loaded
        loads.put(machine,
            TResources.createResourceUsage(
                TResources.createResourceVector(1000, 4), 100));
      }
    }
    return waterLevelPolicy.assignTasks(tasks, loads);
  }


  /** Return the set of machines which we want to probe for a given job. */
  private Collection<InetSocketAddress> getMachinesToProbe(
      Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks, int sampleRatio) {
    HashMap<InetAddress, InetSocketAddress> addrToSocket = Maps.newHashMap();
    Set<InetSocketAddress> probeSet = Sets.newHashSet();

    for (InetSocketAddress node: nodes) {
      addrToSocket.put(node.getAddress(), node);
    }
    List<TTaskSpec> unconstrainedTasks = Lists.newLinkedList();

    List<TTaskSpec> taskList = Lists.newArrayList(tasks);
    Collections.shuffle(taskList);
    for (TTaskSpec task : taskList) {
      List<InetSocketAddress> interests = Lists.newLinkedList();
      if (task.preference != null && task.preference.nodes != null) {
        Collections.shuffle(task.preference.nodes);
        for (String node : task.preference.nodes) {
          try {
            InetAddress addr = InetAddress.getByName(node);
            if (addrToSocket.containsKey(addr)) {
              interests.add(addrToSocket.get(addr));
            } else {
              LOG.warn("Placement constraint for unknown node " + node);
              LOG.warn("Node address: " + addr);
              String knownAddrs = "";
              for (InetAddress add: addrToSocket.keySet()) {
                knownAddrs += " " + add.getHostAddress();
              }
              LOG.warn("Know about: " + knownAddrs);
            }
          } catch (UnknownHostException e) {
            LOG.warn("Got placement constraint for unresolvable node " + node);
          }
        }
      }
      // We have constraints
      if (interests.size() > 0) {
        int myProbes = 0;
        for (InetSocketAddress addr: interests) {
          if (!probeSet.contains(addr)) {
            probeSet.add(addr);
            myProbes++;
          }
          if (myProbes >= sampleRatio) break;
        }
      } else { // We are not constrained
        unconstrainedTasks.add(task);
      }
    }

    List<InetSocketAddress> nodesLeft = Lists.newArrayList(
        Sets.difference(Sets.newHashSet(nodes), probeSet));
    Collections.shuffle(nodesLeft);
    int numAdditionalNodes = Math.min(unconstrainedTasks.size() * sampleRatio,
        nodesLeft.size());
    probeSet.addAll(nodesLeft.subList(0, numAdditionalNodes));
    return probeSet;
  }
}
