package org.apache.hadoop.mapred.wfapp;
import java.util.*;
public class WorkflowMng {
	private Map<WorkflowApp, Long> workflowDeadline = new HashMap<WorkflowApp,Long>();
	private Map<String, WorkflowApp> workflowNames = new HashMap<String, WorkflowApp>();
	private Map<WorkflowApp, List<String>> criticalPaths = new HashMap<WorkflowApp,List<String>>();
	
	WorkflowMng(){
		
	}
	
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
