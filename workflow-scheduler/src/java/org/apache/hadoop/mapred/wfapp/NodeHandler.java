package org.apache.hadoop.mapred.wfapp;

import java.util.ArrayList;
import java.util.List;

//TODO javadoc
public abstract class NodeHandler {

    public interface Context {

        public NodeDef getNodeDef();

        public String getExecutionPath();

        public String getParentExecutionPath(String executionPath);

        public String getSignalValue();

        public void setVar(String name, String value);

        public String getVar(String name);

        public void setTransientVar(String name, Object value);

        public Object getTransientVar(String name);

        public String createExecutionPath(String name);

        //can be called only from exit(), creation of execPaths is automatic
        //when a handler returns more than one transition.
        public void deleteExecutionPath();

        //must be used by multiExit
        public String createFullTransition(String executionPath, String transition);

        public void killJob();

        public void completeJob();

//        public LiteWorkflowInstance getProcessInstance();
    }

    private static final String VISITED = "visited";

    public static String getLoopFlag(String nodeName) {
        return nodeName+ VISITED;
    }

    public void loopDetection(Context context) throws WorkflowException {
        String flag = getLoopFlag(context.getNodeDef().getName());
        if (context.getVar(flag) != null) {
            
        }
        context.setVar(flag, "true");
    }

    // TRUE means immediate exit, false means has to be signal
    public abstract boolean enter(Context context) throws WorkflowException;

    // the return list contains executionPath#transition, important for fork
    public List<String> multiExit(Context context) throws WorkflowException {
        List<String> transitions = new ArrayList<String>(1);
        String transition = exit(context);
        if (transition != null) {
            transitions.add(context.createFullTransition(context.getExecutionPath(), transition));
        }
        return transitions;
    }


    public abstract String exit(Context context) throws WorkflowException;

    public void kill(Context context) {
    }

    public void fail(Context context) {
    }
}