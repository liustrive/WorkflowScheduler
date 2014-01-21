package org.apache.hadoop.mapred;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.wfapp.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	private Map<JobID, JobInProgress> waitingJobs = new HashMap<JobID, JobInProgress>();
	private Map<JobID, String> jobInWorkflow = new HashMap<JobID, String>();
	private Map<WorkflowApp, ArrayList<ArrayList<String>>> criticalPaths = new HashMap<WorkflowApp,ArrayList<ArrayList<String>>>();
	private Map<JobID, Integer> completedJobs = new HashMap<JobID,Integer>();
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
	public String getWfAppNameofJob(JobID jobid){
		return jobInWorkflow.get(jobid);
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
		int minCompleteTasks = 1;
		int minTotalTasks = 1;
		for(List<String> jobNames : criticalNames){
			for(String jobName : jobNames){
				JobID id = app.getNode(jobName).getJobId();
				JobInProgress job = waitingJobs.get(id);
				if(job!=null){
					numCompleteTasks+=job.finishedMaps()+job.finishedReduces();
					numTotalTasks+=job.desiredMaps()+job.desiredReduces();
				}
				else{
					Integer completedTaskofJob = completedJobs.get(id);
					if(completedTaskofJob!=null){
						numCompleteTasks+=completedTaskofJob.intValue();
						numTotalTasks +=completedTaskofJob.intValue();
					}
					else{// this job havn't been submit yet , get task num from configuration file
						NodeConfig jobConf = app.getJobConfig(jobName);
						if(jobConf.mapTaskNum != 0){
							numTotalTasks += jobConf.mapTaskNum;
							if(jobConf.reduceTaskNum != 0){
								numTotalTasks += jobConf.reduceTaskNum;
							}
							else{
								numTotalTasks += jobConf.mapTaskNum;
							}
						}
						
					}
				}
			}
			if(numCompleteTasks/numTotalTasks < minCompleteTasks/minTotalTasks){
				minCompleteTasks = numCompleteTasks;
				minTotalTasks = numTotalTasks;
			}
		}
		appProc.numCriticalCompletedTotalTask = minCompleteTasks;
		appProc.numCriticalTotalTasks = minTotalTasks;
		long currentTime = System.currentTimeMillis();
		if(appProc.deadline != 0){ // if deadline is 0, this function didn't even should be called!
			appProc.eagerness = appProc.numCriticalCompletedTotalTask*(appProc.deadline-appProc.startTime)/(appProc.numCriticalTotalTasks*(currentTime -appProc.startTime));
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
			avaiableJobs = app.availableJobs();
			if(avaiableJobs.size()>0){
				jobtoInit = new ArrayList<JobInProgress>();
				for(JobID jobid : avaiableJobs){
					jobtoInit.add(waitingJobs.get(jobid));
				}
			}
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
					avaiableJobs = app.availableJobs();
					if(avaiableJobs.size()>0){
						jobtoInit = new ArrayList<JobInProgress>();
						for(JobID jobid : avaiableJobs){
							jobtoInit.add(waitingJobs.get(jobid));
						}
					}
					jobInWorkflow.put(jobID, wfAppName);
				}
				else{ // new wf app
					LOG.info("New wf submited. WorkflowApp: " + wfAppName+ ", actionName: "+actionName);
					workflowApps.put(wfAppName, app);
					app.setActionNodeId(actionName, jobID);
					avaiableJobs = app.availableJobs();
					if(avaiableJobs.size()>0){
						jobtoInit = new ArrayList<JobInProgress>();
						for(JobID jobid : avaiableJobs){
							jobtoInit.add(waitingJobs.get(jobid));
						}
					}
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
		List<JobID> avaiableJobs = null;
		JobID jobid = job.getJobID();
		String wfAppName = jobInWorkflow.get(jobid);
		WorkflowApp app = workflowApps.get(wfAppName);
		addCompleteTasks(job,app);
		
		app.nodeFinished(jobid);
		if(!app.finished()){
			avaiableJobs = app.availableJobs();
			if(avaiableJobs.size()>0){
				jobtoInit = new ArrayList<JobInProgress>();
				for(JobID avaiableid : avaiableJobs){
					jobtoInit.add(waitingJobs.get(avaiableid));
				}
			}
		}
		else{
			deleteWorkflowApp(app);
		}
		// delete from waiting jobs in case of JobInProgress being changed outside
		completedJobs.put(jobid, job.desiredMaps()+job.desiredReduces());
		waitingJobs.remove(jobid);
		
		return jobtoInit;
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
	
}
