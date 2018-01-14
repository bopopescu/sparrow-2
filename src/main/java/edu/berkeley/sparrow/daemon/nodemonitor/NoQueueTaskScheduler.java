package edu.berkeley.sparrow.daemon.nodemonitor;

import edu.berkeley.sparrow.daemon.util.TResources;
import edu.berkeley.sparrow.thrift.TFullTaskId;
import edu.berkeley.sparrow.thrift.TResourceUsage;


/**
 * A {@link TaskScheduler} which makes tasks instantly available for launch.
 * 
 * This does not perform any resource management or queuing. It can be used for 
 * applications which do not want Sparrow to perform any explicit resource management 
 * but still want Sparrow to launch tasks.
 */
public class NoQueueTaskScheduler extends TaskScheduler {

  @Override
  void handleSubmitTask(TaskDescription task, String appId, boolean isFake) {
    // Make this task instantly runnable
    makeTaskRunnable(task);
  }


  @Override
  TResourceUsage getResourceUsage(String appId) {
    TResourceUsage out = new TResourceUsage();
    out.resources = TResources.subtract(capacity, getFreeResources());
    
    // We never queue
    out.queueLength = 0;
    return out;
  }


  @Override
  protected void handleTaskCompleted(TFullTaskId taskId) {
    // Do nothing
  }

}
