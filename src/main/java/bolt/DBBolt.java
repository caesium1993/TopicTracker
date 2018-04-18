package bolt;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import utils.FileWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class DBBolt extends BaseRichBolt {
    private FileWriter fw;
    private boolean flag = false;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.fw = new FileWriter((String) map.get("dirTopicTable"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if(flag){
           Utils.sleep(1000);
            return;
        } else {
            String p = tuple.getStringByField("positive posts");
            if(p.equals("finished")){
                try {
                    flag = true;
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return;
            } else if(StringUtils.isNotBlank(p)){
                try {
                    fw.writeSingleLine(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    public void cleanup(){
        int i=0;
        while(i<5){
            System.out.println("******************all done********************");
            i++;
        }
    }
}
