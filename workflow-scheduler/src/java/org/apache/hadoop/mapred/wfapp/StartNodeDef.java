package org.apache.hadoop.mapred.wfapp;
import java.util.Arrays;

/**
 * Workflow lite start node definition.
 */
public class StartNodeDef extends ControlNodeDef {

    /**
     * Reserved name fo the start node. <p/> It is an invalid token, it will never match an application node name.
     */
    public static final String START = ":start:";

    /**
     * Default constructor.
     */
    public StartNodeDef() {
    }

    /**
     * Create a start node definition.
     *
     * @param klass control node handler class.
     * @param transitionTo transition on workflow start.
     */
    public StartNodeDef(Class<? extends ControlNodeHandler> klass, String transitionTo) {
        super(START, "", klass, Arrays.asList(ParamChecker.notEmpty(transitionTo, "transitionTo")));
    }

    /**
     * Start node handler. <p/> It does an immediate transition to the transitionTo node.
     */
    public static class StartNodeHandler extends NodeHandler {

        public boolean enter(Context context) throws WorkflowException {
            if (!context.getSignalValue().equals(StartNodeDef.START)) {
                throw new WorkflowException(ErrorCode.E0715, context.getSignalValue());
            }
            return true;
        }

        public String exit(Context context) {
            return context.getNodeDef().getTransitions().get(0);
        }

        public void kill(Context context) {
        }

        public void fail(Context context) {
        }
    }

}