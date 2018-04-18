package bolt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.shade.org.apache.commons.collections.ListUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import type.TumblrPost;

import java.util.List;
import java.util.Map;

public class FilterBolt extends BaseRichBolt {
   // private Hashtable<Long, type.TumblrPost> posts = null;
    //private List<String> posts = null;  //the list of on-topic posts
    public String[] keywords;
    public OutputCollector collector;
    private Gson gson;

    public final String ALL_MATCH_STREAM = "all match"; //contains all keywords
    public final String SOME_MATCH_STREAM = "some match";//contains some keyswords

    public FilterBolt(String[] keywords) {
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //this.posts = new ArrayList<>();
        int length = this.keywords.length;
        for(int i=0;i<length;i++){
            keywords[i] = this.keywords[i].replaceAll("_"," ").toLowerCase();
        }
        this.gson = new GsonBuilder().create();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        /**
         * check whether stream is finished
         */
        if(input.getStringByField("raw posts").equals("finished")){
            System.out.println("FilterBolt: no more lines");
            Utils.sleep(10000);
            this.collector.emit(ALL_MATCH_STREAM,new Values("finished","finished"));
            this.collector.emit(SOME_MATCH_STREAM,new Values("finished","finished"));
            return;
        }

        TumblrPost post = gson.fromJson(input.getStringByField("raw posts"), TumblrPost.class);
        String text = post.getText();
        List<String> tags = post.getTags();
        if(StringUtils.isBlank(text)){
            return;
        }
        text = text.replaceAll("[^-a-zA-Z0-9 ]"," ").replaceAll("  +"," ");

        int textFlag = 0;
        int tagsFlag = 0;
        int length = this.keywords.length;
        for(String kw: this.keywords){
            if(hasKeyWord(text,kw)){
                textFlag++;
            }
            if(hasKeyWord(tags,kw)){
                tagsFlag++;
            }
        }

        if(tagsFlag==length||textFlag==length){
            this.collector.emit(ALL_MATCH_STREAM, new Values(text, input.getStringByField("raw posts")));
        } else if(tagsFlag==0&&textFlag==0){
            return;
        }else if((tagsFlag>0&tagsFlag<length)||(textFlag>0&textFlag<length)){
            this.collector.emit(SOME_MATCH_STREAM, new Values(text, input.getStringByField("raw posts")));
        }
        Utils.sleep(10);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ALL_MATCH_STREAM, new Fields("text", "raw posts"));
        outputFieldsDeclarer.declareStream(SOME_MATCH_STREAM, new Fields("text", "raw posts"));
    }

    private boolean hasKeyWord(List<String>tags, String kw){
        boolean flag = false;
        if(tags.isEmpty()){
            return false;
        }
        for(String tag:tags){
            if(tag.equalsIgnoreCase(kw)){
                flag=true;
                break;
            }
        }
        return flag;
    }

    /**
     *  check whether the text contains keyword
     * @param text
     * @param kw
     * @return
     */
    private boolean hasKeyWord(String text, String kw) {
        if(text.contains(kw)){
            return true;
        } else{
            return false;
        }
    }

}
