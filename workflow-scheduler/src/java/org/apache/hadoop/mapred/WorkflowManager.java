package org.apache.hadoop.mapred;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.wfapp.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.Collection;
import java.io.*;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

public class WorkflowManager {
	private static final Log LOG = LogFactory.getLog(WorkflowManager.class);
	private static final String SEPERATOR = ".";
	public static final String WFXMLFILE = "workflow.xml";
	private Map<String, Long> workflowDeadline = new HashMap<String,Long>();
	private Map<String, WorkflowApp> workflowApps = new HashMap<String, WorkflowApp>();
	//2014- wf progress update
	private Map<JobID,JobCompleteInfo> completedJobs = new HashMap<JobID,JobCompleteInfo>();
	//
	private Map<JobID, JobInProgress> waitingJobs = new HashMap<JobID, JobInProgress>();
	private Map<JobID, String> jobInWorkflow = new HashMap<JobID, String>();
	private Map<JobID, PathProgressInfo> jobInWfPath = new HashMap<JobID,PathProgressInfo>();
	private Map<WorkflowApp, ArrayList<ArrayList<String>>> criticalPaths = new HashMap<WorkflowApp,ArrayList<ArrayList<String>>>();
//	private Map<JobID, Integer> completedJobs = new HashMap<JobID,Integer>();
	private Map<JobID, WorkflowAppAction> jobAsActions = new HashMap<JobID,WorkflowAppAction>();
	private List<String> waitingQueue = new ArrayList<String>();
//	private List<String> workingQueue = new ArrayList<String>();
	private Map<String,String> triggerQueueMap = new HashMap<String,String>();
	FileSystem fs;
	WorkflowManager(){
		try{
			fs = FileSystem.get(new Configuration());
		}
		catch(IOException ioe){
			LOG.info("error getting filesystem.");
		}
	}
//	private interface WorkflowApp{
//
//		List<String> getCriticalPath();
//		
//	}
	public class JobCompleteInfo{
		public long avgCompleteTaskTime;
		public int numMapTasks;
		public int numRedTasks;
		public long avgMapTaskTime;
		public long avgReduceUnitTime;
		public long startTime;
		public long finishTime;
		
	}
	public class PathProgressInfo{
		public long dueTime;
		public float progressRate;
		public float numSlotNeeded;
		public long avgMapTime;
		public int numTaskRemain;
		public int numTotalTasks;
	}
	public boolean isWorkflowJob(JobID jobid){
		if(jobInWorkflow.containsKey(jobid)){
			return true;
		}
		else
			return false;
	}
	public Collection<WorkflowApp> getAllWfApp(){
		return workflowApps.values();
	}
	public WorkflowApp getWorkflowApp(String appName){
		return workflowApps.get(appName);
	}
	/**
	 * return the workflow app name of job, never return null, "" instead.
	 * @param jobid
	 * @return
	 */
	public String getWfAppNameofJob(JobID jobid){
		if(jobInWorkflow.containsKey(jobid))
			return jobInWorkflow.get(jobid);
		else{
			return "";
		}
	}
	public PathProgressInfo getJobInPathRate(JobID jobid){
		return jobInWfPath.get(jobid);
	}
	public int getWfJobRank(JobID jobid){
		return workflowApps.get(jobInWorkflow.get(jobid)).getNodeRank(jobid);
	}
	public String parseWFName(WorkflowApp app){
		String appName = app.getName();
		String wfUser = app.getUser();
		return wfUser+SEPERATOR+appName;
	}
	public String parseWFName(String appName,String userName){
		return userName+SEPERATOR+appName;
	}
	public String getAppName(String WFName){
		return WFName.substring(WFName.indexOf(SEPERATOR)+1);
	}
	public void setJobStart(JobInProgress job){
		JobID jobid = job.getJobID();
		String workflowAppName = jobInWorkflow.get(jobid);
		WorkflowApp app = workflowApps.get(workflowAppName);
		app.nodeStarted(jobid);
		// if wfApp haven't set start time yet, set as current time
		if(app.getAppProcess().startTime == 0){
			app.getAppProcess().startTime = System.currentTimeMillis();
		}
	}
	private ArrayList<ArrayList<String>> parseCriticalNames(List<String> cirticalString,WorkflowApp app){
		ArrayList<ArrayList<String>> criticalNames = new ArrayList<ArrayList<String>>();
		for(String critical : cirticalString){
			String[] jobNames = critical.split(WorkflowApp.PATH_SEPARATOR);
			ArrayList<String> newNames = new ArrayList<String>();
			for(String jobName : jobNames){
				newNames.add(jobName);
			}
			criticalNames.add(newNames);
		}
		
		return criticalNames;
	}
	private ArrayList<ArrayList<JobID>> parseCriticalIDs(List<String> cirticalString,WorkflowApp app){
		ArrayList<ArrayList<JobID>> criticalIDs = new ArrayList<ArrayList<JobID>>();
		for(String critical : cirticalString){
			String[] jobNames = critical.split(WorkflowApp.PATH_SEPARATOR);
			ArrayList<JobID> newIDs = new ArrayList<JobID>();
			for(String jobName : jobNames){
				JobID id = app.getNode(jobName).getJobId();
				newIDs.add(id);
			}
			criticalIDs.add(newIDs);
		}
		
		return criticalIDs;
	}
	/**
     * Returns the average time it takes to finish a map. The result is based
     * on all previously completed map tasks.
     */
   private long getAverageMapTime(List<TaskInProgress> completedMaps) {

      int numCompletedMaps = completedMaps.size();
      long mapTime = getTotalTaskTime(completedMaps);
      return mapTime / numCompletedMaps;
    }
   /**
    * print all workflow jobs's running info.
    */
   	public void dumpJobInfo(){
   		String dumpStr = "WF jobs Info: ";
   		for(JobInProgress j : waitingJobs.values()){
   			PathProgressInfo ppr = jobInWfPath.get(j.getJobID());
   			dumpStr+=j.getProfile().getJobName();
   			dumpStr+=" MAP (finished:"+j.finishedMaps()+",total:"+j.desiredMaps()+",running:"+j.runningMaps()
   			+" ) REDUCE(running:"+ j.runningReduces()+", total:"+j.desiredReduces()
   			+")";
   			if(ppr!=null){
   				dumpStr+="progressRate:"+ ppr.progressRate + ", avgTaskTime:"+ ppr.avgMapTime+ ", numSlotNeeded:"+ppr.numSlotNeeded;
   			}
   			 
   		}
   		LOG.info(dumpStr);
   	}
    private long getTotalTaskTime(List<TaskInProgress> tips) {
      long totalTime = 0;
      for (TaskInProgress tip: tips) {
        long start = tip.getExecStartTime();
        long finish = tip.getExecFinishTime();
        totalTime += finish - start;
      }
      return totalTime;
    }

	
	/**
	 * get the workflowapp which the job is in, and return the current process rate
	 * This function may be a little costly, and job in the critical path that havn't been submit only count task num that defined in the conf file
	 * @param jobid
	 * @return
	 */
	public WorkflowAppProcess getWorkflowProcessRate(WorkflowApp app){
		WorkflowAppProcess appProc = app.getAppProcess();
		ArrayList<ArrayList<String>> criticalNames = criticalPaths.get(app);
		if(criticalNames == null){
			List<String> paths = app.getCriticalPath();
			criticalNames = parseCriticalNames(paths,app);
			criticalPaths.put(app, criticalNames);
		}
		// find the slowest critical path, min {sum_completeTasks/sumTotalTasks}
		int numCompleteTasks = 0;
		int numTotalTasks = 0;
		int numRunningMaps = 0;
		long runningTime = 0;
		long maxDueTime = 0;
		long timeUsed = 0;
		long currentTime = System.currentTimeMillis();
		int index = 0;
		for(List<String> jobNames : criticalNames){
			for(String jobName : jobNames){
				JobID id = app.getNode(jobName).getJobId();
				JobInProgress job = waitingJobs.get(id);
				if(job!=null){
					numCompleteTasks+=job.finishedMaps()+job.finishedReduces();
					// get the exactly working progress of one path.
					//compute finished ones..
					Vector<TaskInProgress> vct = job.reportTasksInProgress(true, true);
					long totaltime = getTotalTaskTime(vct);
					timeUsed+=totaltime;
					numTotalTasks += job.desiredMaps();
					numCompleteTasks += job.finishedMapTasks;
					//compute running ones..
					Vector<TaskInProgress> vctrunning  = job.reportTasksInProgress(true, false);
					for(TaskInProgress tip : vctrunning){
						if(tip.isRunning() && !tip.isComplete()){
							long start = tip.getExecStartTime();
							long passed = currentTime - start;
							numRunningMaps+=1;
							runningTime += passed;
						}
					}
				}
				else{
					JobCompleteInfo jc = completedJobs.get(id);
					if(jc!=null){
						numCompleteTasks+=jc.numMapTasks;
						numTotalTasks +=numCompleteTasks;
						timeUsed += jc.avgCompleteTaskTime * numCompleteTasks;
					}
					else{// this job havn't been submit yet , get task num from configuration file
						NodeConfig jobConf = app.getJobConfig(jobName);
						if(jobConf.mapTaskNum != 0){
							numTotalTasks += jobConf.mapTaskNum;
//							if(jobConf.reduceTaskNum != 0){
//								numTotalTasks += jobConf.reduceTaskNum;
//							}
//							else{
//								numTotalTasks += jobConf.mapTaskNum;
//							}
						}
						
					}
				}
			}
			index++;
			if(numCompleteTasks<1){
				// havn't start yet, will be scheduled due to node rank
				continue;
			}
			// collect information of every path on the workflow application
			long avg = timeUsed/numCompleteTasks;
			// deadline  = totaltasks * (current - start) / completetasks + start
			long dueTime = numTotalTasks*(currentTime-app.getAppProcess().startTime)/(runningTime/avg+numCompleteTasks);
			float progressRate = (runningTime/avg + numCompleteTasks)/numTotalTasks;
			PathProgressInfo ppi = new PathProgressInfo();
			ppi.dueTime = dueTime;
			ppi.progressRate = progressRate;
			ppi.avgMapTime = avg;
			ppi.numTaskRemain = numTotalTasks - numCompleteTasks - (int)(runningTime/avg);
			ppi.numTotalTasks = numTotalTasks;
			appProc.pathProgressInfo.put(index,ppi);
			if(maxDueTime < dueTime){
				maxDueTime = dueTime;
			}
		}
		LOG.info("WorkflowProcessRate Info: maxDueTime: "+ maxDueTime);
		// if no maxDueTime set, it means the workflow app is just started.
		if(maxDueTime == 0){
			// do nothing by far.
		}
		else{
			for(List<String> jobNames : criticalNames){
				int index_set = 0;
				// reset progressrate of jobs on every path, running jobs only.
				if(appProc.pathProgressInfo.containsKey(index_set)){
					PathProgressInfo proInfo = appProc.pathProgressInfo.get(index_set);
					long timeRemains = maxDueTime - currentTime + app.getAppProcess().startTime;
					float turns = (float)timeRemains/proInfo.avgMapTime;
					if(maxDueTime != proInfo.dueTime)
						proInfo.numSlotNeeded = proInfo.numTaskRemain/turns;
					else
						proInfo.numSlotNeeded = proInfo.numTotalTasks;
					
					for(String jobName : jobNames){
						JobID id = app.getNode(jobName).getJobId();
						JobInProgress job = waitingJobs.get(id);
						if(job!=null){
							jobInWfPath.put(id, proInfo);
							LOG.info("Path in workflow Info: jobname: "+ job.getProfile().getJobName()
									+ ". ProgressInfo: (avgMapTime,dueTime,progressRate,numTaskRemain,numSlotNeed)=("+
									proInfo.avgMapTime+","+proInfo.dueTime+","+proInfo.progressRate+","+proInfo.numTaskRemain+","+ proInfo.numSlotNeeded);
						}
					}
				}
			}
		}
		
		
		if(appProc.deadline != 0){ // if deadline is 0, no need to compute
			appProc.eagerness = maxDueTime/appProc.deadline;
		}
		return appProc;
	}
	/**
	 * get the workflowapp which the job is in, and return the current process rate
	 * This function may be a little costly, and job in the critical path that havn't been submit only count task num that defined in the conf file
	 * @param jobid
	 * @return
	 */
	public WorkflowAppProcess getWorkflowProcessRate(JobID jobid){
		WorkflowApp app = workflowApps.get(jobInWorkflow.get(jobid));
		return getWorkflowProcessRate(app);
	}
	private Map<String,String> jobNameParser(String jobName){
		Map<String,String> jobWFandAction = new HashMap<String,String>();
		String[] split = jobName.split(":");
		for(String tokens : split){
			String [] token = tokens.split("=");
			if(token.length==2){
				jobWFandAction.put(token[0], token[1]);
			}
		}
		return jobWFandAction;
	}
	public List<JobInProgress> jobAdded(JobInProgress job,Path xmlFilePath){
		List<JobInProgress> jobtoInit = null;
		List<JobID> avaiableJobs = null;
		JobID jobID = job.getJobID();
		// job.getJobName() normally is string of numbers
		// however it can be specified, "oozie:launcher:T=map-reduce:W=wordcount-wf:A=wordside:ID=0000025-140109225606903-oozie-liu-W" 
		String jobName = job.getProfile().getJobName();
		Map<String,String> jobWFandAction = jobNameParser(jobName);
		String actionName = jobWFandAction.get("A");
		String wfName = jobWFandAction.get("W");
		String wfAppName = "";
		if(wfName!=null){
			wfAppName = parseWFName(wfName,job.getUser());
		}
		
		waitingJobs.put(jobID, job);
		WorkflowApp app = null;
		// if the job name is specified
		if(waitingQueue.contains(wfAppName)){
			LOG.info("New wf job found, WorkflowApp:" +wfAppName+", actionName: "+ actionName);
			app = workflowApps.get(wfAppName);
			app.setActionNodeId(actionName, jobID);
			jobtoInit = new ArrayList<JobInProgress>();
			jobtoInit.add(job);
			/*
			avaiableJobs = app.availableJobs();
			if(avaiableJobs.size()>0){
				jobtoInit = new ArrayList<JobInProgress>();
				for(JobID jobid : avaiableJobs){
					if(waitingJobs.get(jobid)!=null){
					jobtoInit.add(waitingJobs.get(jobid));
					LOG.info("add job to init: " + jobid.toString());
					}
					else{
						LOG.info("trying to init some job none existed!");
					}
				}
			}
			*/
			jobInWorkflow.put(jobID, wfAppName);
		}
		// if job name not set correctly or its the first wf job submited, will parse the workflow.
		else{
			Reader br=null;
			WorkflowXmlParser parser = null;

	
			
			try{
				br=new InputStreamReader(fs.open(xmlFilePath));
				parser = new WorkflowXmlParser(null,ControlNodeHandler.class,DecisionNodeHandler.class,ActionNodeHandler.class);
				app = parser.validateAndParse(br);
				app.setUser(job.getUser());
				actionName = app.getWfFileActionName();
				wfAppName = parseWFName(app);
				
				if(waitingQueue.contains(wfAppName)){ // wf already loaded
					LOG.info("New wf(existed) job found by parsing xml. WorkflowApp: " + wfAppName+ ", actionName: "+actionName);
					app = workflowApps.get(wfAppName);
					app.setActionNodeId(actionName, jobID);
					jobtoInit = new ArrayList<JobInProgress>();
					jobtoInit.add(job);
					/*
					avaiableJobs = app.availableJobs();
					if(avaiableJobs.size()>0){
						jobtoInit = new ArrayList<JobInProgress>();
						for(JobID jobid : avaiableJobs){
							if(waitingJobs.get(jobid)!=null){
							jobtoInit.add(waitingJobs.get(jobid));
							LOG.info("add job to init: " + jobid.toString());
							}
							else{
								LOG.info("trying to init some job none existed!");
							}
						}
					}
					*/
					jobInWorkflow.put(jobID, wfAppName);
				}
				else{ // new wf app
					LOG.info("New wf submited. WorkflowApp: " + wfAppName+ ", actionName: "+actionName);
					//god I even forgot this
					waitingQueue.add(wfAppName);
					//
					workflowApps.put(wfAppName, app);
					app.setActionNodeId(actionName, jobID);
					jobtoInit = new ArrayList<JobInProgress>();
					jobtoInit.add(job);
					/*
					avaiableJobs = app.availableJobs();
					if(avaiableJobs.size()>0){
						jobtoInit = new ArrayList<JobInProgress>();
						for(JobID jobid : avaiableJobs){
							if(waitingJobs.get(jobid)!=null){
							jobtoInit.add(waitingJobs.get(jobid));
							LOG.info("add job to init: " + jobid.toString());
							}
							else{
								LOG.info("trying to init some job none existed!");
							}
						}
					}
					*/
					jobInWorkflow.put(jobID, wfAppName);
				}
			}
			catch(IOException ioe){
				LOG.error("error open xml cache file: "+ xmlFilePath.toString()+"\n"+ioe.getMessage());
			}
			catch(WorkflowException wfe){
				LOG.error("error parsing workflow xml file: "+ xmlFilePath.toString()+"\n"+wfe.getMessage());
			}
		}
		if(jobtoInit==null){
			jobtoInit = new ArrayList<JobInProgress>();
			LOG.error("The workflowManager somehow didn't get the right avaiable job, adding it manually.");
			jobtoInit.add(job);
		}
		return jobtoInit;
	}
	private void deleteWorkflowApp(WorkflowApp app){
		List<JobID> jobids = app.allActionNodesID();
		for(JobID id : jobids){
			waitingJobs.remove(id);
			jobInWorkflow.remove(id);
		}
		String appName = parseWFName(app);
		workflowDeadline.remove(appName);
		waitingQueue.remove(appName);
	}
	private int addCompleteTasks(JobInProgress job, WorkflowApp app){
		int completeTasks = job.desiredTasks();
		return app.addCompleteJob(completeTasks);
	}
	public List<JobInProgress> jobCompleted(JobInProgress job){
		List<JobInProgress> jobtoInit = null;
//		List<JobID> avaiableJobs = null;
		JobID jobid = job.getJobID();
		String wfAppName = jobInWorkflow.get(jobid);
		WorkflowApp app = workflowApps.get(wfAppName);
		addCompleteTasks(job,app);
		
		app.nodeFinished(jobid);
		if(!app.finished()){
//			avaiableJobs = app.availableJobs();
//			if(avaiableJobs.size()>0){
//				jobtoInit = new ArrayList<JobInProgress>();
//				for(JobID avaiableid : avaiableJobs){
//					jobtoInit.add(waitingJobs.get(avaiableid));
//				}
//			}
		}
		else{
			LOG.info("Workflow app: "+ parseWFName(app)+" has finished, deleing from list.");
			deleteWorkflowApp(app);
		}
		// delete from waiting jobs in case of JobInProgress being changed outside
		//completedJobs.put(jobid, job.desiredMaps()+job.desiredReduces());
		addtoCompletedJobs(job);
		waitingJobs.remove(jobid);
		// avaiablejob not needed right now
		jobtoInit = null;
		return jobtoInit;
	}
	private void addtoCompletedJobs(JobInProgress job){
		JobCompleteInfo jc = new JobCompleteInfo();
		jc.numMapTasks = job.numMapTasks;
		jc.numRedTasks = job.numReduceTasks;
		jc.avgMapTaskTime = getAverageMapTime(job.reportTasksInProgress(true, true));
		jc.startTime = job.launchTime;
		jc.finishTime = job.finishTime;
		completedJobs.put(job.getJobID(), jc);
		
	}
//	public List<JobID> getCriticalPath(String appName){
//		WorkflowApp app = workflowApps.get(appName);
//		if(app!=null){
//			if(criticalPaths.get(app)==null){
//				List<String> paths = app.getCriticalPath();
//				criticalPaths.put(app, paths);
//				return paths;
//			}
//			else
//				return criticalPaths.get(app);
//		}
//		else{
//			return null;
//		}
//	}
	private class WorkflowAppAction{
		public String wfAppName;
		public String actionName;
		public int rank;
	}
}
