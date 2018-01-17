package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.google.common.collect.Maps;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.daemon.util.ThriftClientPool;
import edu.berkeley.sparrow.thrift.InternalService.AsyncClient;
import edu.berkeley.sparrow.thrift.TResourceUsage;
import edu.berkeley.sparrow.thrift.TTaskSpec;

/***
 * A {@link TaskPlacer} implementation which randomly distributes tasks across
 * backends. Note that if there are fewer backends than tasks, this will assign multiple
 * tasks to some backends.
 */
public class RandomTaskPlacer implements TaskPlacer {
  private RandomAssignmentPolicy policy = new RandomAssignmentPolicy();
  @Override
  public Collection<TaskPlacementResponse> placeTasks(String appId,
      String requestId, Collection<InetSocketAddress> nodes, Collection<TTaskSpec> tasks, HashMap<String, Double> workerSpeedMap)
          throws IOException {
    Map<InetSocketAddress, TResourceUsage> nodeUsage = Maps.newHashMap();
    for (InetSocketAddress socket : nodes) {
      TResourceUsage usage = new TResourceUsage();
      // Resource info is ignored by random policy
      usage.queueLength = 0;
      usage.resources = TResources.createResourceVector(0, 0);
      nodeUsage.put(socket, usage);
    }
    return policy.assignTasks(tasks, nodeUsage);
  }

  @Override
  public void initialize(Configuration conf, ThriftClientPool<AsyncClient> clientPool) {
  }

}
