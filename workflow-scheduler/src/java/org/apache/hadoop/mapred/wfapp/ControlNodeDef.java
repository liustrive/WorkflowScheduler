package org.apache.hadoop.mapred.wfapp;
import java.util.List;

/**
 * Node definition for control nodes: START/END/KILL/FORK/JOIN.
 */
public abstract class ControlNodeDef extends NodeDef {

    ControlNodeDef() {
    }

    @SuppressWarnings("unchecked")
    public ControlNodeDef(String name, String conf, Class<? extends ControlNodeHandler> controlHandlerClass,
                          List <String> transitions) {
        super(name, conf, controlHandlerClass, transitions);
    }

}
