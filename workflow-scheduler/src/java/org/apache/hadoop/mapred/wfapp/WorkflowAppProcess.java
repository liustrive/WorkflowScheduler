package org.apache.hadoop.mapred.wfapp;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.mapred.WorkflowManager.PathProgressInfo;

public class WorkflowAppProcess {
	public int numCriticalTotalTasks = 0;
	public int numCompletedJobTasks = 0;
	public int numCriticalCompletedTotalTask = 0;
//	public int numAppTotalTasks = 0;
	public String appName;
	public long startTime = 0;
	public long deadline = 0;
	public Map<Integer,PathProgressInfo> pathProgressInfo;
	/**
	 * numCriticalCompletedTotalTask*(deadline-startTime)/(numCriticalTotalTasks*(nowTime-startTime))
	 */
	public float eagerness; 
	WorkflowAppProcess(String name){
		appName = name;
		pathProgressInfo = new HashMap<Integer,PathProgressInfo>();
	}

}
