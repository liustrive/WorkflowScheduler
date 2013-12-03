package org.apache.hadoop.mapred.wfapp;
//TODO javadoc
public abstract class DecisionNodeHandler extends NodeHandler {

    @Override
    public final boolean enter(Context context) throws WorkflowException {
        start(context);
        return false;
    }

    @Override
    public final String exit(Context context) throws WorkflowException {
        end(context);
        String signalValue = context.getSignalValue();
        if (context.getNodeDef().getTransitions().contains(signalValue)) {
            return signalValue;
        }
        else {
            throw new WorkflowException(ErrorCode.E0721, context.getNodeDef().getName(), signalValue);
        }
    }

    public abstract void start(Context context) throws WorkflowException;

    public abstract void end(Context context) throws WorkflowException;

}