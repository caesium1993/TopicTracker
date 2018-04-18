package spout;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import type.TumblrPost;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class TumblrSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    /*private String consumer_key;
    private String consumer_secret;
    private String access_token;
    private String token_secret;*/
    private String[] keywords;

    private boolean flag = false;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    //private int index=0;


    public TumblrSpout(String[] keywords) {
        /*this.consumer_key = consumer_key;
        this.consumer_secret = consumer_secret;
        this.access_token = access_token;
        this.token_secret = token_secret;*/
        this.keywords = keywords;
    }

    /*public spout.TumblrSpout(String[] keywords, Hashtable<Long, type.TumblrPost> posts) {
        /*this.consumer_key = consumer_key;
        this.consumer_secret = consumer_secret;
        this.access_token = access_token;
        this.token_secret = token_secret;
        this.keywords = keywords;
        this.posts = posts;
    }*/

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            this.fileReader = new FileReader(map.get("dir").toString());
            this.bufferedReader = new BufferedReader(fileReader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        //this.tuples.addAll(posts.values());
    }

    public void nextTuple() {
        if(flag){
            Utils.sleep(1000);
            return;
        }
        String str;
        try {
            if(StringUtils.isNotBlank((str = bufferedReader.readLine()))){
                    this.collector.emit(new Values(str));
            } else{
                    flag = true;
                    this.bufferedReader.close();
                    this.collector.emit(new Values("finished"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*public void nextTuple() {
        if(flag){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return;
        } else{
            type.TumblrPost post = tuples.get(index);
            this.collector.emit(new Values(post.getText()));
            index++;
        }
    }*/

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("raw posts"));
    }
}
