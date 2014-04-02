package org.apache.hadoop.mapred.wfapp;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//TODO javadoc
public class WorkflowApp{
    private String name;
    private String user;
    private String definition;
    private String actionName;
    private WorkflowAppProcess appProcess;
    private int jobtofinish = 0;
    private int priority = 0; // 0 for normal, 1 for high, -1 for low, -10 for linger
    private String triggerJob;
    private boolean isTrigger = false;
    private Map<String, NodeDef> nodesMap = new LinkedHashMap<String, NodeDef>();
    private Map<JobID, String> nodesIdName = new HashMap<JobID, String>();
    private Map<String, Integer> nodeRankMap = new HashMap<String, Integer>();
    private Map<String, NodeConfig> jobConfig = new HashMap<String, NodeConfig>();
    private List<String> criticalPath = new ArrayList<String>();
    private boolean complete = false;
    private boolean ranked = false; // if the nodes' rank have been set
    static final Log LOG = LogFactory.getLog(WorkflowApp.class);
    public static String PATH_SEPARATOR = "/";


    WorkflowApp() {
    }

    public WorkflowApp(String name, String definition, StartNodeDef startNode) {
        this.name = ParamChecker.notEmpty(name, "name");
        this.definition = ParamChecker.notEmpty(definition, "definition");
        nodesMap.put(StartNodeDef.START, startNode);
        appProcess = new WorkflowAppProcess(name);
    }
    
    public boolean equals(WorkflowApp other) {
        return !(other == null || getClass() != other.getClass() || !getName().equals(other.getName()));
    }

    public int hashCode() {
        return name.hashCode();
    }
    //liu added
    public int addCompleteJob(int num){
    	appProcess.numCompletedJobTasks += num;
    	return appProcess.numCompletedJobTasks;
    }
    public int getCompleteJobTasksNum(){
    	return appProcess.numCompletedJobTasks;
    }
    public void setCriticalCompletedTaskNum(int num){
    	appProcess.numCriticalCompletedTotalTask = num;
    }
    public void setCriticalTotalTaskNum(int num){
    	appProcess.numCriticalTotalTasks = num;
    }
//    public void setAppTotalTaskNum(int num){
//    	appProcess.numAppTotalTasks = num;
//    }
    public WorkflowAppProcess getAppProcess(){
    	return appProcess;
    }
    public boolean finished(){
    	if(jobtofinish<=0){
    		return true;
    	}
    	else
    		return false;
    }
    public void setUser(String u){
    	user = u;
    }
    public String getUser(){
    	return user;
    }
    /**
     * set deadline of workflowApp, the time string will be convert to
     * the number of milliseconds that have elapsed since January 1, 1970
     * @param time
     */
    public List<JobID> allActionNodesID(){
    	List<JobID> jobids = new ArrayList<JobID>();
    	jobids.addAll(nodesIdName.keySet());
    	return jobids;
    }
    public void setDeadline(String time){
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    	try{
    		Date date = dateFormat.parse(time);
    		appProcess.deadline = date.getTime();
    	}
    	catch(Exception e){
    		LOG.info("wrong deadline time format!: "+ e.getMessage());
    	}
    }
    public long getDeadline(){
    	return appProcess.deadline;
    }
    public String getWfFileActionName(){
    	return actionName;
    }
    public void setActionName(String name){
    	actionName = name;
    }
    public void setPriority(int pri){
    	priority = pri;
    }
    public int getPriority(){
    	return priority;
    }
    public boolean isTriggerWorkflow(){
    	return isTrigger;
    }
    public void setTriggerJob(String jobName){
    	isTrigger = true;
    	triggerJob = jobName;
    }
    public String getTriggerJob(){
    	return triggerJob;
    }
    public void addJobConfig(String jobName,NodeConfig conf){
    	jobConfig.put(jobName,conf);
    }
    /**
     * will return null if no job found
     * @param jobName
     * @return
     */
    public NodeConfig getJobConfig(String jobName){
    	return jobConfig.get(jobName);
    }
    /**
     *  set the rank of nodes (R(n_i) = 1+ max_(n_j in succ(n_i)) R(n_j) )
     *  
     */
    public void setAppRank(){
    	NodeDef node = getNode(StartNodeDef.START);
    	List<String> transitions = node.getTransitions();
    	if(transitions.size()>1)
    		LOG.info("more than one transitions in start node!");
    	String transition = transitions.get(0);
    	NodeDef tmpNode = getNode(transition);
    	updateNodeRank(tmpNode);
    	
    	ranked = true;
    }
  
    /**
     * return and set the node rank of nodes in workflow app
     * all control nodes do not add up the rank
     * @param node
     * @return
     */
    private int updateNodeRank(NodeDef node){
    	int nodeRank = 0;
    	if(node instanceof EndNodeDef || node instanceof KillNodeDef){
    		nodeRank = 0;
    	}
    	else if(node instanceof ForkNodeDef){ // control node will equals the children's max rank
    		List<String> trans = node.getTransitions();
    		for(String tran : trans){
    			int tmpRank = updateNodeRank(getNode(tran));
    			if(nodeRank < tmpRank){
    				nodeRank = tmpRank;
    			}
    		}
    	}
    	else if(node instanceof JoinNodeDef){ 
    		List<String> trans = node.getTransitions();
    		String tran = trans.get(0);
    		nodeRank = updateNodeRank(getNode(tran));	
    		
    	}
    	else if(node instanceof DecisionNodeDef){
    		List<String> trans = node.getTransitions();
    		for(String tran : trans){
    			int tmpRank = updateNodeRank(getNode(tran));
    			if(nodeRank < tmpRank){
    				nodeRank = tmpRank;
    			}
    		}
    	}
    	else if(node instanceof ActionNodeDef){
	    	List<String> transitions = node.getTransitions();
	    	NodeDef tmpNode = getNode(transitions.get(0)); // ok to
	    	nodeRank = updateNodeRank(tmpNode)+1;
	    	nodeRankMap.put(node.getName(), nodeRank);
    	}
    	node.setRank(nodeRank);
    	
    	return nodeRank;
    }
    /**
     * set the MapReduce job id of ActionNodes, return false if no job found or the job isn't an ActionNode
     * @param id
     * @return
     */
    public boolean setActionNodeId(String jobName,JobID id){
    	NodeDef node = getNode(jobName);
    	if(node==null)
    	{
    		LOG.info("job: "+jobName+ " isn't in the list of workflowapp");
    		return false;
    	}
    	else{
    		if(node instanceof ActionNodeDef){
    			node.setJobId(id);
    			if(nodesIdName.get(id)!=null)
    				LOG.info("job: "+ jobName +"'s id set more than once!");
    			nodesIdName.put(id, jobName);
        		return true;
    		}
    		else{
    			LOG.info("job: "+jobName + " is not an aciton node");
    			return false;
    		}
    	}
    }
    /**
     * get the MapReduce job name of ActionNodes, return null if no job found or the job isn't an ActionNode
     * @param id
     * @return
     */
    public String getNodeNameById(JobID id){
    	String nodeName = nodesIdName.get(id);
    	if(nodeName == null){
    		LOG.info("job id: "+id+ " isn't in the list of workflowapp");
    		return null;
    	}
    	else{
    		return nodeName;
    	}
    	
    }
    /**
     * return node rank. if not set yet, run setNodeRank first. if no node name found, return -1
     * @param jobName
     * @return
     */
    public int getNodeRank(String jobName){
    	if(!ranked){
    		setAppRank();
    	}
    	int rank = -1;
    	NodeDef node = getNode(jobName);
    	if(node==null){
    		return rank = -1;
    	}
    	else
    		return rank = node.getRank();
    }
    public int getNodeRank(JobID jobid){
    	String jobName = nodesIdName.get(jobid);
    	return getNodeRank(jobName);
    }
    private boolean checkNodeRank(NodeDef node){
    	Iterator it = nodeRankMap.entrySet().iterator();
    	while(it.hasNext()){
    		Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
    		if(pair.getValue().intValue() >node.getRank()){
    			if(!getNode(pair.getKey()).isStarted())
    				return false;
    		}
    	}
    	return true;
    }
    private boolean checkDependency(NodeDef node){
    	
    	return true;
    }
    // haven't done yet
    /**
     * ask if an action node should start.
     * action i shouldn't start until all nodes whose rank > rank_i started
     * @param id
     * @return
     */
    public boolean shouldStart(JobID id){
    	String nodeName = getNodeNameById(id);
    	if(nodeName != null){
    		NodeDef node = getNode(nodeName);
    		if(checkNodeRank(node)){
    			if(checkDependency(node)){
    				return true;
    			}
    			else
    				return false;
    		}
    		else
    			return false;
    	}
    	else
    		return false;
    }
    /**
     * This function do not check the dependency before the node. The dependency is maintained by recursive.
     * @param node
     * @param list
     */
    private void isAvailableActionNode(NodeDef node, List<JobID> list, String forkJoinPaths,Map<String,Integer> joinMap){
    	
    	if(node instanceof ActionNodeDef){
    		if(node.isStarted()){
    			String tran = node.getTransitions().get(0);
    			isAvailableActionNode(getNode(tran),list,forkJoinPaths,joinMap);
    		}
    		else{
    			if(checkNodeRank(node)){
    				list.add(node.getJobId());
    			}
    			else{
    				return;
    			}
    		}
    	}
    	else if(node instanceof DecisionNodeDef){
    		List<String> transitions = node.getTransitions();
    		for(String transition : transitions){
    			isAvailableActionNode(getNode(transition),list,forkJoinPaths,joinMap);
    		}
    	}
    	else if(node instanceof ForkNodeDef){
    		String newForkJoinPath = forkJoinPaths+PATH_SEPARATOR+node.getName(); // when fork, add on the path
    		List<String> transitions = node.getTransitions();
    		for(String transition : transitions){
    			isAvailableActionNode(getNode(transition),list,newForkJoinPath,joinMap);
    		}
    	}
    	else if(node instanceof JoinNodeDef){
    		String parentFork = getParentFork(forkJoinPaths);
    		NodeDef forkNode = getNode(parentFork);
    		List<String> trans = forkNode.getTransitions();
    		if(joinMap.get(node.getName())==null){
    			joinMap.put(node.getName(), trans.size()-1);
    		}
    		else{
    			int forkNumLast = joinMap.get(node.getName());
    			if(forkNumLast==1){//remove parent forkNode name from forkJoinPaths,remove joinNode name from joinMap
    				forkJoinPaths = getParentPath(forkJoinPaths);
    				joinMap.remove(node.getName());
    				String transition = node.getTransitions().get(0);
    				isAvailableActionNode(getNode(transition),list,forkJoinPaths,joinMap);
    			}
    			else{
    				forkNumLast = forkNumLast-1;
    				joinMap.put(node.getName(), forkNumLast);
    			}
    		}
    	}
    	else{
    		return;
    	}
    }
    private String getParentFork(String path){
    	return (path.length()==0)?null:path.substring(path.lastIndexOf(PATH_SEPARATOR)+1);
    }
    private String getParentPath(String path){
    	if(path.lastIndexOf(PATH_SEPARATOR)==-1)
    		return new String("");
    	else
    		return path.substring(0,path.lastIndexOf(PATH_SEPARATOR));
    }
    /**
     * return list of available ActionNodes' id
     * @return
     */
    public List<JobID> availableJobs(){
    	
    	List<JobID> jobList = new ArrayList<JobID>();
    	NodeDef node = getNode(StartNodeDef.START);
    	String forkJoinPaths = StartNodeDef.START;
    	String transition = node.getTransitions().get(0);
    	Map<String,Integer> joinMap = new HashMap<String,Integer>();
    	isAvailableActionNode(getNode(transition),jobList,forkJoinPaths,joinMap);
    	return jobList;
    }
    
    public void nodeStarted(JobID id){
    	String nodeName = getNodeNameById(id);
    	if(nodeName != null){
    		getNode(nodeName).start();
    	}
    }
    public void nodeFinished(JobID id){
    	String nodeName = getNodeNameById(id);
    	if(nodeName != null){
    		getNode(nodeName).finish();
    		jobtofinish-=1;
    	}
    }
    public WorkflowApp addNode(NodeDef node) throws WorkflowException {
        ParamChecker.notNull(node, "node");
        if (complete) {
            throw new WorkflowException(ErrorCode.E0704, name);
        }
        if (nodesMap.containsKey(node.getName())) {
            throw new WorkflowException(ErrorCode.E0705, node.getName());
        }
        if (node.getTransitions().contains(node.getName())) {
            throw new WorkflowException(ErrorCode.E0706, node.getName());
        }
        nodesMap.put(node.getName(), node);
        if (node instanceof EndNodeDef) {
            complete = true;
        }
        if(node instanceof ActionNodeDef){
        	jobtofinish+=1;
        }
        ranked = false;
        return this;
    }

    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public Collection<NodeDef> getNodeDefs() {
        return Collections.unmodifiableCollection(nodesMap.values());
    }

    public NodeDef getNode(String name) {
        return nodesMap.get(name);
    }

    public void validateWorkflowIntegrity() {
        //TODO traverse wf, ensure there are not cycles, no open paths, and one END
    }

    public void validateTransition(String name, String transition) {
        ParamChecker.notEmpty(name, "name");
        ParamChecker.notEmpty(transition, "transition");
        NodeDef node = getNode(name);
        if (!node.getTransitions().contains(transition)) {
            throw new IllegalArgumentException("invalid transition");
        }
    }


    
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        //dataOutput.writeUTF(definition);
        //writeUTF() has limit 65535, so split long string to multiple short strings
        List<String> defList = divideStr(definition);
        dataOutput.writeInt(defList.size());
        for (String d : defList) {
            dataOutput.writeUTF(d);
        }
        dataOutput.writeInt(nodesMap.size());
        for (NodeDef n : getNodeDefs()) {
            dataOutput.writeUTF(n.getClass().getName());
            n.write(dataOutput);

        }
    }

    /**
     * To split long string to a list of smaller strings.
     *
     * @param str
     * @return List
     */
    private List<String> divideStr(String str) {
        List<String> list = new ArrayList<String>();
        int len = 20000;
        int strlen = str.length();
        int start = 0;
        int end = len;

        while (end < strlen) {
            list.add(str.substring(start, end));
            start = end;
            end += len;
        }

        if (strlen <= end) {
            list.add(str.substring(start, strlen));
        }
        return list;
    }

  
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        //definition = dataInput.readUTF();
        //read the full definition back
        int defListSize = dataInput.readInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < defListSize; i++) {
            sb.append(dataInput.readUTF());
        }
        definition = sb.toString();

        int numNodes = dataInput.readInt();
        for (int x = 0; x < numNodes; x++) {
            try {
                String nodeDefClass = dataInput.readUTF();
                NodeDef node = (NodeDef) ReflectionUtils.newInstance(Class.forName(nodeDefClass), null);
                node.readFields(dataInput);
                addNode(node);
            }
            catch (WorkflowException ex) {
                throw new IOException(ex);
            }
            catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }
    
    private void setCriticalPath(NodeDef node,String path){
    	if(node instanceof EndNodeDef || node instanceof KillNodeDef){
    		criticalPath.add(path);
    	}
    	else if(node instanceof ForkNodeDef||node instanceof DecisionNodeDef){ // control node will equals the children's max rank
    		List<String> trans = node.getTransitions();
    		List<NodeDef> maxRankNodes = new ArrayList<NodeDef>();
    		int maxRank = 0;
    		for(String tran : trans){
    			NodeDef tmpNode = getNode(tran);
    			if(tmpNode.getRank()>maxRank){
    				//get every path by now.
//    				maxRankNodes.clear();
    				maxRankNodes.add(tmpNode);
    			}
    			else if(tmpNode.getRank()==maxRank){
    				maxRankNodes.add(tmpNode);
    			}
    		}
    		for(NodeDef nodes : maxRankNodes){
    			setCriticalPath(nodes,path);
    		}
    	}
    	else if(node instanceof JoinNodeDef){ 
    		List<String> trans = node.getTransitions();
    		String tran = trans.get(0);
    		setCriticalPath(getNode(tran),path);
    	}
    	else if(node instanceof ActionNodeDef){
	    	List<String> transitions = node.getTransitions();
	    	NodeDef tmpNode = getNode(transitions.get(0)); // ok to
	    	String newPath = (path.length()==0)?node.getName():path+PATH_SEPARATOR+node.getName();
	    	setCriticalPath(tmpNode,newPath);
    	}
    }
	public List<String> getCriticalPath() {
		// TODO Auto-generated method stub
		if(criticalPath.size()==0){
			setCriticalPath(getNode(StartNodeDef.START),new String(""));
			return criticalPath;
		}
		else
			return criticalPath;

	}

}