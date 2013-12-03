package org.apache.hadoop.mapred.wfapp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node definition for JOIN control node.
 */
public class JoinNodeDef extends ControlNodeDef {

    JoinNodeDef() {
    }

    public JoinNodeDef(String name, Class<? extends ControlNodeHandler> klass, String transition) {
        super(name, "", klass, Arrays.asList(transition));
    }

    public static class JoinNodeHandler extends NodeHandler {

        public void loopDetection(Context context) throws WorkflowException {
            String flag = getLoopFlag(context.getNodeDef().getName());
            if (context.getVar(flag) != null) {
                throw new WorkflowException(ErrorCode.E0709, context.getNodeDef().getName());
            }
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count == 0) {
                context.setVar(flag, "true");
            }
        }

        public boolean enter(Context context) throws WorkflowException {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            String forkCount = context.getVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath);
            if (forkCount == null) {
                throw new WorkflowException(ErrorCode.E0720, context.getNodeDef().getName());
            }
            int count = Integer.parseInt(forkCount) - 1;
            if (count > 0) {
                context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath, "" + count);
                context.deleteExecutionPath();
            }
            else {
                context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + parentExecutionPath, null);
            }
            return (count == 0);
        }

        public List<String> multiExit(Context context) {
            String parentExecutionPath = context.getParentExecutionPath(context.getExecutionPath());
            // NOW we delete..
            context.deleteExecutionPath();

            String transition = context.getNodeDef().getTransitions().get(0);
            String fullTransition = context.createFullTransition(parentExecutionPath, transition);
            List<String> transitions = new ArrayList<String>(1);
            transitions.add(fullTransition);
            return transitions;
        }

        public String exit(Context context) {
            throw new UnsupportedOperationException();
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }
    }
}