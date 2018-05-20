package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import utils.FileWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class DBBolt extends BaseRichBolt {
    private FileWriter fw;
    private FileWriter fw_off;
    //private String dirOffTopic = "E://data/storm/table_off_topic.json";
    private static String[] keywords;
    private static int round = 0;
    private Map<Integer, String[]> results;
    private static int count;//determine whether this is a new round

    private final String ALL_MATCH_STREAM = "all match";
    private final String ON_TOPIC_STREAM = "on topic";

    public DBBolt(String[] keywords) {
        DBBolt.keywords = keywords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.count = 0;
        this.results = new HashMap<>();
        this.results.put(this.round, keywords);
        String dir = setTableDir();
        String dirOffTopic = setOffTableDir();
        try {
            this.fw = new FileWriter(dir);
            this.fw_off = new FileWriter(dirOffTopic);
            StringBuilder kwLine = new StringBuilder("Round " + Integer.toString(this.round) + ":");
            for(String kw: keywords){
                kwLine.append(" ").append(kw);
            }
            this.fw.writeSingleLine(kwLine.toString());
            this.fw_off.writeSingleLine(kwLine.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    @Override
    public void execute(Tuple tuple) {
        System.out.println("DBBolt Round"+this.round+": "+ Arrays.asList(keywords));
        System.out.println("DBBolt: "+tuple.getSourceStreamId()+tuple.getValue(0));

        if(this.round>this.count){
            this.count=this.round;
            this.results.put(this.round, keywords);
            try {
                this.fw.close();
                this.fw_off.close();
                Utils.sleep(500L);
                String dir = setTableDir();
                String dirOffTopic = setOffTableDir();
                this.fw = new FileWriter(dir);
                this.fw_off = new FileWriter(dirOffTopic);
            } catch (IOException e) {
                e.printStackTrace();
            }
            StringBuilder kwLine = new StringBuilder("Round " + Integer.toString(this.round) + ":");
            for(String kw: keywords){
                kwLine.append(" ").append(kw);
            }
            try {
                fw_off.writeSingleLine(kwLine.toString());
                fw.writeSingleLine(kwLine.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        writeIntoTable(tuple);


    }

    private void writeIntoTable(Tuple tuple) {

        if(tuple.getSourceStreamId().equals(ALL_MATCH_STREAM)){
            try {
                fw.writeSingleLine(tuple.getStringByField("raw posts"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(tuple.getSourceStreamId().equals(ON_TOPIC_STREAM)){
            try {
                fw.writeSingleLine(tuple.getStringByField("positive posts"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if(tuple.getSourceStreamId().equals("off topic")){
            try {
                this.fw_off.writeSingleLine(tuple.getStringByField("negative posts"));
                return;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    private String setTableDir() {
        String dir = "E://data/storm/table_round_";
        dir = dir + Integer.toString(this.round)+".json";
        return dir;
    }

    private String setOffTableDir() {
        String dir = "E://data/storm/table_off_topic_round_";
        dir = dir + Integer.toString(this.round)+".json";
        return dir;
    }

    public void setKeywords(String[] keywords) {
        this.keywords = keywords;
    }

    public void setRound(int round) {
        this.round = round;
    }

    public void cleanup(){
        Iterator iterator = results.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry) iterator.next();
            String num = "Round"+Integer.toString((Integer) entry.getKey());
            String[] kws = (String[]) entry.getValue();
            System.out.println(num+": "+Arrays.asList(kws));
        }
    }
}
