/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobQueueJobInProgressListener.JobSchedulingInfo;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapred.wfapp.*;
/**
 * A {@link JobInProgressListener} that maintains the jobs being managed in
 * one or more queues. 
 */
class WorkflowJobQueuesManager extends JobInProgressListener {
  
  private static final Log LOG = LogFactory.getLog(WorkflowJobQueuesManager.class);
  private WorkflowTaskScheduler scheduler;
  private static WorkflowManager workflowManager = new WorkflowManager();
  public static Map<String,WorkflowAppProcess> eagerWfAppProcess = new HashMap<String,WorkflowAppProcess>();
  // Queues in the system
  private Collection<String> jobQueueNames;
  private Map<String, WorkflowSchedulerQueue> jobQueues = 
    new HashMap<String, WorkflowSchedulerQueue>();

  
  WorkflowJobQueuesManager(WorkflowTaskScheduler s) {
    this.scheduler = s;
  }
  
  void setQueues(Map<String, WorkflowSchedulerQueue> queues) {
    this.jobQueues = queues;
    this.jobQueueNames = new ArrayList<String>(queues.keySet());
  }
  private Path getXmlFile(Path filePath) throws IOException{
	  Path xmlFile = null; 
	  
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] status = null;
      try{
       status = fs.listStatus(filePath);
      }
      catch(IOException ioe){
    	  LOG.info("cache filepath: "+filePath.toString()+" does not exist." + ioe.getMessage());
    	  return null;
      }
      if(status!=null){
	      for (int i=0;i<status.length;i++){
	    	  	LOG.info("find cache file: "+ status[i].getPath().getName());
	    	  	if(status[i].getPath().getName().equals(WorkflowManager.WFXMLFILE)){
	    	  		xmlFile = status[i].getPath();
	    	  		LOG.info("find workflow xml file: "+xmlFile.getName());
	    	  	}
	    	  	LOG.info("file path: "+status[i].getPath().toString());
	    	  	// this is how to read the file ...
	//               BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
	//                  String line;
	//                  line=br.readLine();
	//                  while (line != null){
	//                          LOG.info("file: " + line);
	//                          line=br.readLine();
	//                  }
	      }
      }
	  return xmlFile;
  }
  
  Path findXmlFilePath(JobInProgress job){
	  	Path jobFilePath = new Path(job.jobFile);
	    Path jobSubmitDir = jobFilePath.getParent();
	    Path jobDistCacheFiles = JobSubmissionFiles.getJobDistCacheFiles(jobSubmitDir);
	    try{
		    URI[] localFiles = DistributedCache.getCacheFiles(job.getJobConf());
		    if(localFiles==null){
		    	return null;
		    }
		    else{
			    for(int i = 0;i< localFiles.length;i++){
			    	Path file = new Path(localFiles[i].getPath());
			    	LOG.info("DistributedCache: find cache file: "+ file.getName());
			    	if(WorkflowManager.WFXMLFILE.equalsIgnoreCase(file.getName())){
			    		LOG.info("DistributedCache: find workflow xml file: "+file.getName());
			    		return file;
			    	}
			    }
		    }
	    }
	    catch(IOException ioe){
	    	LOG.error("Exception occur in findXmlFileviaDisCache: "+ ioe.getMessage());
	    }
	    Path wfXmlFile = null;
	    //Liu edited 
	    LOG.info("File Path: "+ job.jobFile+ ". JobSubmitDir: " + jobSubmitDir.toString()+". JobDistCacheFiles: "+ jobDistCacheFiles.toString());
	    try{
	    //get DistCacheFiles -files *
	    	wfXmlFile = getXmlFile(jobDistCacheFiles);  
	    }
	    catch(IOException ioe){
	    	LOG.error("error in findXmlFilePath: "+ ioe.getMessage());
	    }
	    return wfXmlFile;
  }
  @Override
  public void jobAdded(JobInProgress job) throws IOException {
    LOG.info("Job " + job.getJobID() + " submitted to queue " + 
        job.getProfile().getQueueName());
    Path xmlFile = findXmlFilePath(job);
    if(xmlFile != null){
    	List<JobInProgress> newjobs = workflowManager.jobAdded(job, xmlFile);
    	String wfAppName = workflowManager.getWfAppNameofJob(job.getJobID());
    	LOG.info("Workflow job: " + job.getProfile().getJobName()+ "is added. Belongs to wfapp: "+ wfAppName);
    	// newjobs will should be null
    	for(JobInProgress newjob : newjobs){
    		 // add job to the right queue
    		//but may contain some null pointers during debug
    		if(newjob==null)
    			continue;
    	    WorkflowSchedulerQueue queue = getQueue(newjob.getProfile().getQueueName());
    	    if (null == queue) {
    	      // job was submitted to a queue we're not aware of
    	      LOG.warn("Invalid queue " + newjob.getProfile().getQueueName() + 
    	          " specified for job" + newjob.getProfile().getJobID() + 
    	          ". Ignoring job.");
    	      return;
    	    }
    	    // --liu-- set workflow job as start
    	    workflowManager.setJobStart(newjob);
    	    int rankInApp = workflowManager.getWfJobRank(newjob.getJobID());
    	    //  
    	    // add job to waiting queue. It will end up in the right place, 
    	    // based on priority, workflowApp it is in.
    	    //queue.addWaitingJob(newjob);
    	    WorkflowJobSchedulingInfo wfJobSchedInfo = new WorkflowJobSchedulingInfo(newjob,wfAppName,rankInApp);
    	    queue.addWaitingJob(newjob, wfJobSchedInfo);
    	    // let scheduler know. 
    	    scheduler.jobAdded(newjob);
    	}
    }
    else{ // not a workflow job, start immediately
    	// add job to the right queue
	    WorkflowSchedulerQueue queue = getQueue(job.getProfile().getQueueName());
	    if (null == queue) {
	      // job was submitted to a queue we're not aware of
	      LOG.warn("Invalid queue " + job.getProfile().getQueueName() + 
	          " specified for job" + job.getProfile().getJobID() + 
	          ". Ignoring job.");
	      return;
	    }
	    // add job to waiting queue. It will end up in the right place, 
	    // based on priority.
	    JobSchedulingInfo jobSchedInfo = new JobSchedulingInfo(job);
	    queue.addWaitingJob(job,jobSchedInfo);
	    // let scheduler know. 
	    scheduler.jobAdded(job);
    }
   
  }

  /*
   * Method removes the jobs from both running and waiting job queue in 
   * job queue manager.
   */
  private void jobCompleted(JobInProgress job, JobSchedulingInfo oldInfo, 
      WorkflowSchedulerQueue queue, int runState) {
    LOG.info("Job " + job.getJobID().toString() + " submitted to queue " 
        + job.getProfile().getQueueName() + " has completed");
    // --Liu-- job complete, workflowManager update new avaiable jobs
    if(workflowManager.isWorkflowJob(job.getJobID())){
    	List<JobInProgress> newjobs = workflowManager.jobCompleted(job);
    	String wfAppName = workflowManager.getWfAppNameofJob(job.getJobID());
    	if(newjobs!=null){
	    	for(JobInProgress newjob : newjobs){
		   		 // add job to the right queue
	    		// may contain some null pointer during debug
	    		if(newjob==null)
	    			continue;
		   	    WorkflowSchedulerQueue tmpqueue = getQueue(newjob.getProfile().getQueueName());
		   	    if (null == tmpqueue) {
		   	      // job was submitted to a queue we're not aware of
		   	      LOG.warn("Invalid queue " + newjob.getProfile().getQueueName() + 
		   	          " specified for job" + newjob.getProfile().getJobID() + 
		   	          ". Ignoring job.");
		   	      return;
		   	    }
		   	    // --liu-- set workflow job as start
		   	    workflowManager.setJobStart(newjob);
		   	    try{
		   	    	int rankInApp = workflowManager.getWfJobRank(newjob.getJobID());
			   	    //  
			   	    // add job to waiting queue. It will end up in the right place, 
			   	    // based on priority, workflow it is in.
			   	    //tmpqueue.addWaitingJob(newjob);
		   	    	WorkflowJobSchedulingInfo wfJobSchedInfo = new WorkflowJobSchedulingInfo(newjob,wfAppName,rankInApp);
		    	    tmpqueue.addWaitingJob(newjob, wfJobSchedInfo);
			   	    // let scheduler know. 
			   	    scheduler.jobAdded(newjob);
		   	    }
		   	    catch(IOException ioe){
		   	    	LOG.error("error in jobCompleted: "+ ioe.getMessage());
		   	    	LOG.error("Complete job: "+ job.getProfile().getJobName()+ " error new avaiablejob: "+ newjob.getProfile().getJobName());
		   	    }
	    	}
    	}
    	// now update the workflow app process rate
    	updateWfAppProcess(wfAppName);
    }
    
    //remove jobs from both queue's a job can be in
    //running and waiting queue at the same time.
    JobInProgress waitingJob = queue.removeWaitingJob(oldInfo, runState);
    JobInProgress initializingJob = 
      queue.removeInitializingJob(oldInfo, runState);
    JobInProgress runningJob = queue.removeRunningJob(oldInfo, runState);
    // let scheduler know if necessary
    // sometimes this isn't necessary if the job was rejected during submission
    if (runningJob != null || initializingJob != null || waitingJob != null) {
      scheduler.jobCompleted(job);
    }
    
  }
  /**
   * update all the wf apps that have set the deadline
   */
  public void updateALLwfAppProcess(){
	  for(WorkflowApp app : (Collection<WorkflowApp>)workflowManager.getAllWfApp()){
		  if(app.getDeadline()!=0){
			  updateWfAppProcess(workflowManager.parseWFName(app));
		  }
	  }
  }
  private void updateWfAppProcess(String wfAppName){
  	WorkflowApp app = workflowManager.getWorkflowApp(wfAppName);
  	if(app!=null){
	if(app.getDeadline()!=0){ // if this app does support deadline
    	if(eagerWfAppProcess.get(wfAppName)==null){
    		WorkflowAppProcess process = workflowManager.getWorkflowProcessRate(app);
    		if(process.eagerness<1){
    			eagerWfAppProcess.put(wfAppName, process);
    		}
    	}
    	else{
    		WorkflowAppProcess process = workflowManager.getWorkflowProcessRate(app);
    		if(process.eagerness>1.2){
    			eagerWfAppProcess.put(wfAppName, process);
    		}
    	}
	}
  	}
  	else{
  		LOG.error(wfAppName + "not found in workflowapps.");
  	}
  }
  public JobSchedulingInfo getWorkflowJobSchedInfo(JobInProgress job){
	  JobSchedulingInfo jobSchedInfo = null;
	  JobID jobid = job.getJobID();
	  if(!workflowManager.isWorkflowJob(jobid))
  		jobSchedInfo = new JobSchedulingInfo(job);
  	else{
  		String wfAppName = workflowManager.getWfAppNameofJob(jobid);
  		int rank = workflowManager.getWfJobRank(jobid);
  		jobSchedInfo = new WorkflowJobSchedulingInfo(job,wfAppName,rank);
  	}
	  return jobSchedInfo;
  }
  // Note that job is removed when the job completes i.e in jobUpated()
  @Override
  public void jobRemoved(JobInProgress job) {}
  
  // This is used to reposition a job in the queue. A job can get repositioned 
  // because of the change in the job priority or job start-time.
  private void reorderJobs(JobInProgress job, JobSchedulingInfo oldInfo, 
      WorkflowSchedulerQueue queue, int runState) {
	  JobSchedulingInfo  jobSchedInfo = getWorkflowJobSchedInfo(job);
    if(queue.removeWaitingJob(oldInfo, runState) != null) {
      try {
        queue.addWaitingJob(job,jobSchedInfo);
      } catch (IOException ioe) {
        // Ignore, cannot happen
        LOG.warn("Couldn't change priority!");
        return;
      }
    }
    if (queue.removeInitializingJob(oldInfo, runState) != null) {
      queue.addInitializingJob(job,jobSchedInfo);
    }
    if(queue.removeRunningJob(oldInfo, runState) != null) {
      queue.addRunningJob(job,jobSchedInfo);
    }
  }
  
  // This is used to move a job from the waiting queue to the running queue.
  private void makeJobRunning(JobInProgress job, JobSchedulingInfo oldInfo, 
                              WorkflowSchedulerQueue queue) {
    // Removing of the job from job list is responsibility of the
    //initialization poller.
    // Add the job to the running queue
    queue.addRunningJob(job,oldInfo);
  }
  
  // Update the scheduler as job's state has changed
  // liu modeified: to support workflowJobSchedulingInfo
  private void jobStateChanged(JobStatusChangeEvent event, 
                               WorkflowSchedulerQueue queue) {
    JobInProgress job = event.getJobInProgress();
    JobSchedulingInfo oldJobStateInfo = null;
    JobID jobid = job.getJobID();
    if(!workflowManager.isWorkflowJob(jobid))
    	oldJobStateInfo = new JobSchedulingInfo(event.getOldStatus());
    else{
    	String wfAppName = workflowManager.getWfAppNameofJob(jobid);
    	int rank = workflowManager.getWfJobRank(jobid);
    	oldJobStateInfo = new WorkflowJobSchedulingInfo(event.getOldStatus(),wfAppName,rank);
    }
    // Check if the ordering of the job has changed
    // For now priority and start-time can change the job ordering
    if (event.getEventType() == EventType.PRIORITY_CHANGED 
        || event.getEventType() == EventType.START_TIME_CHANGED) {
      // Make a priority change
      int runState = job.getStatus().getRunState();
      reorderJobs(job, oldJobStateInfo, queue, runState);
    } else if (event.getEventType() == EventType.RUN_STATE_CHANGED) {
      // Check if the job is complete
      int runState = job.getStatus().getRunState();
      if (runState == JobStatus.SUCCEEDED
          || runState == JobStatus.FAILED
          || runState == JobStatus.KILLED) {
        jobCompleted(job, oldJobStateInfo, queue, runState);
      } else if (runState == JobStatus.RUNNING) {
        makeJobRunning(job, oldJobStateInfo, queue);
        
        // liu modified: if job in a workflow app, update workflow app process
        if(workflowManager.isWorkflowJob(jobid)){
        	String wfAppName = workflowManager.getWfAppNameofJob(jobid);
        	updateWfAppProcess(wfAppName);
        }
      }
    }
  }
  
  @Override
  public void jobUpdated(JobChangeEvent event) {
    JobInProgress job = event.getJobInProgress();
    WorkflowSchedulerQueue queue = getQueue(job.getProfile().getQueueName());
    if (null == queue) {
      // can't find queue for job. Shouldn't happen. 
      LOG.warn("Could not find queue " + job.getProfile().getQueueName() + 
          " when updating job " + job.getProfile().getJobID());
      return;
    }
    
    // Check if this is the status change
    if (event instanceof JobStatusChangeEvent) {
      jobStateChanged((JobStatusChangeEvent)event, queue);
    }
  }
  
  WorkflowSchedulerQueue getQueue(String queue) {
    return jobQueues.get(queue);
  }
  
  Collection<String> getAllQueues() {
    return Collections.unmodifiableCollection(jobQueueNames);
  }
}
