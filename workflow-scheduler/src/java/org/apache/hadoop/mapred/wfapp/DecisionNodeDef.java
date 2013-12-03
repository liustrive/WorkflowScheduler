package org.apache.hadoop.mapred.wfapp;
import java.util.List;

public class DecisionNodeDef extends NodeDef {

    DecisionNodeDef() {
    }

    public DecisionNodeDef(String name, String conf, Class<? extends DecisionNodeHandler> clazz, List<String> evals) {
        super(name, ParamChecker.notNull(conf, "conf"), clazz, evals);
    }


}