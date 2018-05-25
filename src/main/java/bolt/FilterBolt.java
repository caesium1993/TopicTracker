package bolt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import type.TumblrPost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FilterBolt extends BaseRichBolt {

    public static String[] keywords;  //keywords with underline
    public static String[] keywords2;  //keywords with whitespace
    public OutputCollector collector;
    private Gson gson;
    //private int limit;
    //private int count =0;


    public final String ALL_MATCH_STREAM = "all match"; //contains all keywords
    public final String SOME_MATCH_STREAM = "some match";//contains some keyswords

    public FilterBolt(String[] keywords) {
        this.keywords = keywords;
        //this.limit = this.keywords.length*20;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //this.posts = new ArrayList<>();
        this.keywords2 = processKeywords(this.keywords);
        this.gson = new GsonBuilder().create();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        String raw = input.getStringByField("raw posts");
        String text;

        /**
         * if input is blank, return
         */
        if(StringUtils.isNotBlank(raw)){
            text = normalizeRawPost(raw);
        } else{
            Utils.sleep(500L);
            return;
        }

        System.out.println("Filter keywords: "+Arrays.asList(this.keywords));

        int matchFlag = 0;
        int length = keywords.length;
        TumblrPost post = gson.fromJson(raw,TumblrPost.class);

        List<String> match = post.getMatch();
        for(int i=0;i<length;i++){
            if(text.contains(keywords[i])||text.contains(keywords2[i])){
                matchFlag++;
                match.add(keywords[i]);
                post.setMatch(match);
            }
        }

        raw = gson.toJson(post, TumblrPost.class);

        if(matchFlag==length){
            this.collector.emit(ALL_MATCH_STREAM, new Values(text, raw));
        }else if(matchFlag>0){
            this.collector.emit(SOME_MATCH_STREAM, new Values(text, raw));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ALL_MATCH_STREAM, new Fields("text", "raw posts"));
        outputFieldsDeclarer.declareStream(SOME_MATCH_STREAM, new Fields("text", "raw posts"));
    }

    /**
     * normalize the text of raw post
     * append tags to text for modeling
     * @param s
     * @return
     */
    public String normalizeRawPost(String s) {
        System.out.println(s);
        TumblrPost p = gson.fromJson(s, TumblrPost.class);
        String text = preprocessText(p.getText().toLowerCase().
                replaceAll("[^_0-9a-zA-Z- ]","").//!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                replaceAll("__+","").
                replaceAll("[ ]+"," "));
        List<String> tags = removeBlank(p.getTags());
        if((!tags.isEmpty())&&(tags!=null)){
            for (String t:tags){
                text += (" "+t.toLowerCase());
            }
        }

        return text.trim();
    }

    /**
     * normalize the #tag in the text, remove '#' and replace blank in tag with '_'
     * @param text
     * @return
     */
    private static String preprocessText(String text) {
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

    /**
     * replace the blank in tag with '_'
     * @param tags
     * @return
     */
    private static List<String> removeBlank(List<String> tags) {
        if(tags.isEmpty()){
            return tags;
        }
        List<String> results = new ArrayList<>();
        for(String t:tags){
            if(t.contains(" ")){
                results.add(t.trim().replaceAll(" ","_").toLowerCase());
            }else {
                results.add(t.trim().toLowerCase());
            }
        }
        return results;
    }

    /**
     * replace the _ with blank
     * @param keywords
     * @return
     */
    private String[] processKeywords(String[] keywords) {
        int len = keywords.length;
        String[] keywords2 = new String[len];

        for(int i=0;i<len;i++){
            if(StringUtils.isNotBlank(keywords[i])){
                keywords2[i] = keywords[i].replaceAll("_"," ").toLowerCase();
            }
        }

        return keywords2;
    }

    public void setKeywords(String[] keywords) {
        this.keywords = keywords;
        this.keywords2 = processKeywords(this.keywords);
    }
}
