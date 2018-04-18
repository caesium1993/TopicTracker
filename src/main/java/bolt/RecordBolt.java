package bolt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import type.TumblrPost;
import utils.FileWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * this bolt is to record the texts of all positive posts that contain all given keywords to into a txt file
 * and write the posts that match some keywords into a json file
 */
public class RecordBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static FileWriter fw1; //file writer for some match stream
    private static FileWriter fw2; //file writer for training model
    private String dirText4Model;
    private String dirCandiPost;
    private Gson gson;

    public final String ALL_MATCH_STREAM = "all match";
    public final String SOME_MATCH_STREAM = "some match";
    public final String POSITIVE_STREAM = "positive posts";
    public final String ALL_TEXT_SENT = "all text sent";


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.gson = new GsonBuilder().create();

        this.dirText4Model = (String) map.get("dirText4Model");
        try {
            this.fw2 = new FileWriter(this.dirText4Model);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.dirCandiPost =(String) map.get("dirCandiPost");
        try {
            this.fw1 = new FileWriter(this.dirCandiPost);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        /**
         * check whether is is the last line
         */
        if(tuple.getStringByField("raw posts").equals("finished")){
            if(tuple.getSourceStreamId().equals(ALL_MATCH_STREAM)){
                System.out.println("Record Bolt: all text has been recorded");
                this.collector.emit(ALL_TEXT_SENT, new Values(dirText4Model));
                try {
                    fw2.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Utils.sleep(1000);
                return;
            } else if(tuple.getSourceStreamId().equals(SOME_MATCH_STREAM)){
                System.out.println("Record Bolt: some match stream has been recorded");
                try {
                    fw1.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Utils.sleep(1000);
                return;
            }
        } else {
            if(tuple.getSourceStreamId().equals(ALL_MATCH_STREAM)){
                String rawPost = tuple.getStringByField("raw posts");
                String text = preprocessText(tuple.getStringByField("text"));
                System.out.println(rawPost);
                this.collector.emit(POSITIVE_STREAM, new Values(rawPost));
                //write the text into train model file
                TumblrPost post = gson.fromJson(rawPost, TumblrPost.class);
                List<String> tags = post.getTags();
                String tagString = "";
                if(!tags.isEmpty()){
                    for(String tag:tags){
                        String t = tag.replaceAll(" ","_");
                        tagString = tagString+" "+t;
                    }
                }
                try {
                    fw2.writeSingleLine(text+" "+tagString);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } else if(tuple.getSourceStreamId().equals(SOME_MATCH_STREAM)){
                String rawPost = tuple.getStringByField("raw posts");
                try {
                    fw1.writeSingleLine(rawPost);
                    System.out.println(SOME_MATCH_STREAM+": "+rawPost);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private String preprocessText(String text) {
        String textString;
        if(text.contains("#")){
            text = text.replaceAll("# +","#");  //remove the white spaces after #

            String[] temp = text.split("#");
            textString = temp[0];
            for(int i=1;i<temp.length;i++){  //!!!!!!!ignore the first subString since it is likely to be a sentence
                temp[i] = temp[i].trim().replaceAll(" ","_");
                textString += temp[i]+" ";
            }
        } else{
            return text;
        }

        return textString.trim();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(POSITIVE_STREAM,new Fields("positive posts"));
        outputFieldsDeclarer.declareStream(ALL_TEXT_SENT, new Fields("dir text4model"));
    }
}
