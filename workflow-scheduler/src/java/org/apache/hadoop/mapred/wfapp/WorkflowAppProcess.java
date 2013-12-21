package org.apache.hadoop.mapred.wfapp;

public class WorkflowAppProcess {
	public int numCriticalTotalTasks = 0;
	public int numCompletedJobTasks = 0;
	public int numCriticalCompletedTotalTask = 0;
//	public int numAppTotalTasks = 0;
	public String appName;
	public long startTime = 0;
	public long deadline = 0;
	/**
	 * numCriticalCompletedTotalTask*(deadline-startTime)/(numCriticalTotalTasks*(nowTime-startTime))
	 */
	public float eagerness; 
	WorkflowAppProcess(String name){
		appName = name;
	}

}
