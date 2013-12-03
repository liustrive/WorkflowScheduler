package org.apache.hadoop.mapred.wfapp;
import java.util.ArrayList;
import java.util.List;


/**
 * Node handler that provides the necessary workflow logic for control nodes: START/END/KILL/FORK/JOIN.
 */
public abstract class ControlNodeHandler extends NodeHandler {

    public static final String FORK_COUNT_PREFIX = "workflow.fork.";

    /**
     * Called by {@link #enter(Context)} when returning TRUE.
     *
     * @param context workflow context
     * @throws WorkflowException thrown if an error occurred.
     */
    public abstract void touch(Context context) throws WorkflowException;

    @Override
    public boolean enter(Context context) throws WorkflowException {
        boolean doTouch;
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
            if (!context.getSignalValue().equals(StartNodeDef.START)) {
                throw new WorkflowException(ErrorCode.E0715, context.getSignalValue());
            }
            doTouch = true;
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
            doTouch = true;
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
            doTouch = true;
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
            doTouch = true;
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count > 0) {
                context.setVar(FORK_COUNT_PREFIX + parentExecutionPath, "" + count);
                context.deleteExecutionPath();
            }
            else {
                context.setVar(FORK_COUNT_PREFIX + parentExecutionPath, null);
            }
            doTouch = (count == 0);
        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
        if (doTouch) {
            touch(context);
        }
        return false;
    }

    @Override
    public String exit(Context context) throws WorkflowException {
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
            return context.getNodeDef().getTransitions().get(0);
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
            context.completeJob();
            return null;
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
            context.killJob();
            return null;
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
            throw new UnsupportedOperationException();
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            throw new UnsupportedOperationException();
        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
    }

    @Override
    public void loopDetection(Context context)
        throws WorkflowException {
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            String flag = getLoopFlag(context.getNodeDef().getName());
            if (context.getVar(flag) != null) {
                throw new WorkflowException(ErrorCode.E0709, context.getNodeDef().getName());
            }
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count == 0) {
                context.setVar(flag, "true");
            }

        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
    }

    @Override
    public List<String> multiExit(Context context)
        throws WorkflowException {
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
            return super.multiExit(context);
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
            return super.multiExit(context);
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
            return super.multiExit(context);
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
            List<String> transitions = context.getNodeDef().getTransitions();
            context.setVar(FORK_COUNT_PREFIX + context.getExecutionPath(), "" + transitions.size());

            List<String> fullTransitions = new ArrayList<String>(transitions.size());

            for (String transition : transitions) {
                String childExecutionPath = context.createExecutionPath(transition);
                String fullTransition = context.createFullTransition(childExecutionPath, transition);
                fullTransitions.add(fullTransition);
            }
            return fullTransitions;
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            // NOW we delete..
            context.deleteExecutionPath();

            String transition = context.getNodeDef().getTransitions().get(0);
            String fullTransition = context.createFullTransition(parentExecutionPath, transition);
            List<String> transitions = new ArrayList<String>(1);
            transitions.add(fullTransition);
            return transitions;
        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
    }

    @Override
    public void kill(Context context) {
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            //NOP
        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
    }

    @Override
    public void fail(Context context) {
        Class<? extends NodeDef> nodeClass = context.getNodeDef().getClass();
        if (nodeClass.equals(StartNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(EndNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(KillNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(ForkNodeDef.class)) {
            //NOP
        }
        else if (nodeClass.equals(JoinNodeDef.class)) {
            //NOP
        }
        else {
            throw new IllegalStateException("Invalid node type: " + nodeClass);
        }
    }
}
