package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class TempBolt extends BaseRichBolt {
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("all text sent")){
            System.out.println("all text have been sent");
            System.out.println("the path of texts4model is: "+tuple.getStringByField("dir text4model"));
        }else if(tuple.getSourceStreamId().equals("positive posts")){
            System.out.println(tuple.getSourceStreamId()+": "+tuple.getStringByField("positive posts"));
        }
    }

    public void testFilterBolt(Tuple tuple){
        if(tuple.getStringByField("raw posts").equals("finished")){
            System.out.println("all lines finished");
            Utils.sleep(1000);
            return;
        } else{
            if(tuple.getSourceStreamId().equals("all match")){
                System.out.println(tuple.getSourceStreamId()+": "+tuple.getStringByField("text"));
                System.out.println(tuple.getSourceStreamId()+": "+tuple.getStringByField("raw posts"));
            }else if(tuple.getSourceStreamId().equals("some match")){
                System.out.println(tuple.getSourceStreamId()+": "+tuple.getStringByField("text"));
                System.out.println(tuple.getSourceStreamId()+": "+tuple.getStringByField("raw posts"));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("*****************ALL CORRECT******************");
        super.cleanup();
    }
}
