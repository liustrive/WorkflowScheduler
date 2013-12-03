package org.apache.hadoop.mapred;

import org.apache.hadoop.mapred.wfapp.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class WorkflowMng {
	private Map<WorkflowApp, Long> workflowDeadline = new HashMap<WorkflowApp,Long>();
	private Map<String, WorkflowApp> workflowNames = new HashMap<String, WorkflowApp>();
	private Map<WorkflowApp, List<String>> criticalPaths = new HashMap<WorkflowApp,List<String>>();
	private List<String> waitingQueue = new ArrayList<String>();
	private List<String> workingQueue = new ArrayList<String>();
	private List<String> triggerQueue = new ArrayList<String>();
	WorkflowMng(){
		
	}
//	private interface WorkflowApp{
//
//		List<String> getCriticalPath();
//		
//	}
	public List<String> getCriticalPath(String appName){
		WorkflowApp app = workflowNames.get(appName);
		if(app!=null){
			if(criticalPaths.get(app)==null){
				List<String> paths = app.getCriticalPath();
				criticalPaths.put(app, paths);
				return paths;
			}
			else
				return criticalPaths.get(app);
		}
		else{
			return null;
		}
	}
	
}
