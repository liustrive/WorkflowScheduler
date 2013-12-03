package org.apache.hadoop.mapred.wfapp;

import java.util.List;

//TODO javadoc
public abstract class ActionNodeHandler extends NodeHandler {
    public static final String OK = "OK";
    public static final String ERROR = "ERROR";

    @Override
    public final boolean enter(Context context) throws WorkflowException {
        start(context);
        return false;
    }

    @Override
    public final String exit(Context context) throws WorkflowException {
        end(context);
        List<String> transitions = context.getNodeDef().getTransitions();
        String signalValue = context.getSignalValue();
        if (OK.equals(signalValue)) {
            return transitions.get(0);
        }
        else {
            if (ERROR.equals(signalValue)) {
                return transitions.get(1);
            }
        }
        throw new WorkflowException(ErrorCode.E0722, context.getNodeDef().getName());
    }

    public abstract void start(Context context) throws WorkflowException;

    public abstract void end(Context context) throws WorkflowException;

}