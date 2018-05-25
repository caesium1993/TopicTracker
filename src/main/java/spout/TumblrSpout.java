package spout;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class TumblrSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    //private boolean flag = false;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private static int round = 0;
    private int tem;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.tem = round;
        try {
            this.fileReader = new FileReader(map.get("dirRawPost").toString());
            this.bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        System.out.println("Spout Round: "+round);

        /**
         * first check round num, if begin a new round, wait 5 seconds
         */
        if(this.round>this.tem){
            this.tem=this.round;
            Utils.sleep(3000L);
            return;
        }
        String str;
        try {
            if(StringUtils.isNotBlank((str = bufferedReader.readLine()))){
                    this.collector.emit(new Values(str));
            } else{
                    Utils.sleep(3000L);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("raw posts"));
    }

    public void setRound(int round) {
        this.round = round;
    }
}
