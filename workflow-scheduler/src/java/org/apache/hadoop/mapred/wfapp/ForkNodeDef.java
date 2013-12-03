package org.apache.hadoop.mapred.wfapp;
import java.util.ArrayList;
import java.util.List;

/**
 * Node definition for FORK control node.
 */
public class ForkNodeDef extends ControlNodeDef {

    ForkNodeDef() {
    }

    public ForkNodeDef(String name, Class<? extends ControlNodeHandler> klass,
                       List<String> transitions) {
        super(name, "", klass, transitions);
    }

    public static class ForkNodeHandler extends NodeHandler {

        public boolean enter(Context context) {
            return true;
        }

        // the return list contains (parentExecutionPath/transition#transition)+
        public List<String> multiExit(Context context) {
            List<String> transitions = context.getNodeDef().getTransitions();
            context.setVar(ControlNodeHandler.FORK_COUNT_PREFIX + context.getExecutionPath(), "" + transitions.size());

            List<String> fullTransitions = new ArrayList<String>(transitions.size());

            for (String transition : transitions) {
                String childExecutionPath = context.createExecutionPath(transition);
                String fullTransition = context.createFullTransition(childExecutionPath, transition);
                fullTransitions.add(fullTransition);
            }
            return fullTransitions;
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