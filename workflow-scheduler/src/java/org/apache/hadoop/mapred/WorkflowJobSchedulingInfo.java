
package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.JobQueueJobInProgressListener.JobSchedulingInfo;

class WorkflowJobSchedulingInfo extends JobSchedulingInfo{

    private String workflowAppName;
    private int rankInApp;
    
    public WorkflowJobSchedulingInfo(JobInProgress jip,String wfAppName,int appRank) {
      this(jip.getStatus(),wfAppName,appRank);
    }
    
    public WorkflowJobSchedulingInfo(JobStatus status,String wfAppName,int appRank) {
	    super(status);

	    workflowAppName = wfAppName;
	    rankInApp = appRank;
    }
    

    String getAppName(){return workflowAppName;}
    int getAppRank(){return rankInApp;}
    
    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != WorkflowJobSchedulingInfo.class) {
        return false;
      } else if (obj == this) {
        return true;
      }
      else if (obj instanceof WorkflowJobSchedulingInfo) {
    	  WorkflowJobSchedulingInfo that = (WorkflowJobSchedulingInfo)obj;
        return (super.getJobID().equals(that.getJobID()) && 
                super.getStartTime() == that.getStartTime() && 
                super.getPriority() == that.getPriority() && 
                this.rankInApp == that.rankInApp && 
                this.workflowAppName == that.workflowAppName);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int)(super.getJobID().hashCode() * super.getPriority().hashCode() + super.getStartTime());
    }

  }
 