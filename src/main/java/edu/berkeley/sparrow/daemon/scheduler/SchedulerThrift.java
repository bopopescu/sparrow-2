package edu.berkeley.sparrow.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import edu.berkeley.sparrow.daemon.SparrowConf;
import edu.berkeley.sparrow.daemon.util.Hostname;
import edu.berkeley.sparrow.daemon.util.TServers;
import edu.berkeley.sparrow.thrift.SchedulerService;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TSchedulingRequest;
import edu.berkeley.sparrow.thrift.TTaskPlacement;

/**
 * This class extends the thrift sparrow scheduler interface. It wraps the
 * {@link Scheduler} class and delegates most calls to that class.
 */
public class SchedulerThrift implements SchedulerService.Iface {
  // Defaults if not specified by configuration
  public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
  private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;

  private Scheduler scheduler = new Scheduler();

  /**
   * Initialize this thrift service.
   *
   * This spawns a multi-threaded thrift server and listens for Sparrow
   * scheduler requests.
   */
  public void initialize(Configuration conf) throws IOException {
    SchedulerService.Processor<SchedulerService.Iface> processor =
        new SchedulerService.Processor<SchedulerService.Iface>(this);
    int port = conf.getInt(SparrowConf.SCHEDULER_THRIFT_PORT,
        DEFAULT_SCHEDULER_THRIFT_PORT);
    int threads = conf.getInt(SparrowConf.SCHEDULER_THRIFT_THREADS,
        DEFAULT_SCHEDULER_THRIFT_THREADS);
    String hostname = Hostname.getHostName(conf);
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    scheduler.initialize(conf, addr);
    TServers.launchThreadedThriftServer(port, threads, processor);
  }

  @Override
  public boolean registerFrontend(String app, String socketAddress) {
    return scheduler.registerFrontend(app, socketAddress);
  }

  @Override
  public boolean submitJob(TSchedulingRequest req)
      throws TException {
    return scheduler.submitJob(req);
  }

  @Override
  public List<TTaskPlacement> getJobPlacement(TSchedulingRequest req)
      throws TException {
    try {
      return new ArrayList<TTaskPlacement>(scheduler.getJobPlacement(req));

    }
    catch (IOException e) {
      throw new TException(e.getMessage());
    }
  }

  @Override
  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) throws TException {
    scheduler.sendFrontendMessage(app, taskId, status, message);
  }


  @Override
  public void sendSchedulerMessage(String app, TFullTaskId taskId,
                                  int status, ByteBuffer message) throws TException {
    scheduler.sendSchedulerMessage(app, taskId, status, message);
  }

}
