package edu.berkeley.sparrow.daemon.nodemonitor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;

/** This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and FIFO's whenever outstanding tasks exceed 
 * this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);
  public int maxActiveTasks = 1;
  public AtomicInteger activeTasks = new AtomicInteger(0);
  public LinkedBlockingQueue<TaskDescription> tasks = new LinkedBlockingQueue<TaskDescription>();
  public LinkedBlockingQueue<TaskDescription> fakeTasks = new LinkedBlockingQueue<TaskDescription>();

  public void setMaxActiveTasks(int max) {
    this.maxActiveTasks = max;
  }
  
  @Override
  synchronized void handleSubmitTask(TaskDescription task, String appId, boolean isFake) {
    if (activeTasks.get() < maxActiveTasks) {
      //Makes the input task runnable. Making Fake task runnable only if real  task isn empty and fake task is not.
      // since it's already submitted, we need to make it runnable without any logic? Check
      makeTaskRunnable(task, isFake);
      activeTasks.incrementAndGet();
    } else {
      try {
        if (isFake) {
          fakeTasks.put(task);
        } else {
          tasks.put(task);
        }
      } catch (InterruptedException e) {
        LOG.fatal(e);
      }
    }
  }

  @Override
  protected void handleTaskCompleted(TFullTaskId taskId) {
    activeTasks.decrementAndGet();
    if (!tasks.isEmpty()) {
      makeTaskRunnable(tasks.poll(),false); //Since the task is polled from the nonFake tasks Queue passing in false
    }
    if(tasks.isEmpty() && !fakeTasks.isEmpty()){
      makeTaskRunnable(fakeTasks.poll(), true); //since it's polled from fake task queue, passing true
    }
    activeTasks.incrementAndGet();
  }

  @Override
  TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    // We use one shared queue for all apps here
    out.queueLength = tasks.size();
    //Need to add another queueLength which isn't shared
    return out;
  }

}
