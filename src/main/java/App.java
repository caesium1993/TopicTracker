import bolt.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import spout.TumblrSpout;
import type.TumblrPost;

import java.io.IOException;
import java.util.Hashtable;


public class App {
    /**
     * @author minjiez
     * @version 1.0
     * this is main class of TopicTracker
     */

    static final String CONSUMER_KEY = "5lWEz6FEjlIB5VHxDKLaXBYZMonrzfVhNBqvXsor7pm2c9WRad";
    static final String CONSUMER_SECRET = "sPcMduJDVKZ9tvSbMwevWESq9abx6ClJq0ESWwzyN4I3h4iugr";
    static final String ACCESS_TOKEN = "dun8yJhuPfKTA2VfOaS5dsl59pImmTiVjblMeLNDCU6VI7t6fQ";
    static final String TOKEN_SECRET = "snCACYLsIxqWwJOpgBUTkFYEF2kXckWtBTw4gpJY15GmOfM4WK";

    /**
     * id for Storm
     */

    private static final String SPOUT_ID = "tumblr spout";
    private static final String FILTER_BOLT_ID = "filter bolt";
    private static final String RECORD_BOLT_ID = "record bolt";
    private static final String MODEL_BOLT_ID = "train model bolt";
    private static final String DECIDER_BOLT_ID = "decider bolt";
    private static final String DB_BOLT_ID = "DB bolt";

    public static Hashtable<Long, TumblrPost> posts = new Hashtable<>();
    public static String[] keywords = null;

    public static String dirRawPost ="E://data/post_2.json";
    public static String dirText4Model = "E://data/storm/text_model.txt";
    public static String dirCandiPost = "E://data/storm/candidate_post.json";
    public static String dirStopWords = "E://data/stopList.txt";
    public static String dirOutPutModel = "E://data/storm/post2_model.txt";
    public static String dirTopicTable = "E://data/storm/topicTable.json";

    public static void main(String[] args) throws InterruptedException, IOException {
        //args are the given input keywords
        keywords = args;

        /*ArrayList<String> followingBlogs = new ArrayList<>();

        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

        RequestBlogNameTask blogNameTask = new RequestBlogNameTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET);
        ScheduledFuture<ArrayList<String>> future = pool.schedule(blogNameTask, 1L, TimeUnit.MILLISECONDS);
        boolean flag = false;*/
        /**
         * request posts by given keywords using tagged method
         */
        /*RequestTaggedPostTask taggedPostTask = new RequestTaggedPostTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET, keywords);
        Hashtable<Long, TumblrPost> taggedResult = taggedPostTask.requestByTag();
        if(taggedResult!=null){
            posts.putAll(taggedResult);
        }*/


        /*while(flag==false){
            Thread.sleep(500L);
            if(future.isDone()){
                flag= true;
                try {
                    followingBlogs=future.get();
                    //System.out.println("following blogs size: "+followingBlogs.size());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }*/

        /*int j=1;
        for(String i:followingBlogs){
            System.out.println("["+j+"]"+i);
            j++;
        }*/

        /*RequestPostTask getPostTask = new RequestPostTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET,followingBlogs,posts);
        dirRawPost = getPostTask.getDir();
        long delay = 1000*120;
        ScheduledFuture<?> scheduledFuture = pool.scheduleWithFixedDelay(getPostTask, 10, delay,
                TimeUnit.MILLISECONDS);

        boolean flag2 = false;
        while(!flag2){
            Thread.sleep(3000);
            if(scheduledFuture.isDone()){
                flag2=true;
            }
        }*/
        /*pool.scheduleWithFixedDelay(getPostTask,10L, delay,
                TimeUnit.MILLISECONDS);*/

        /*while(true){
            Thread.sleep(15000L);
            System.out.println("this is main posts list: "+posts.size());
        }*/
        //Thread.sleep(5000);

        /**
         * set up Storm topology
         */
        TumblrSpout spout = new TumblrSpout(keywords);
        FilterBolt filterBolt = new FilterBolt(keywords);
        RecordBolt recordBolt = new RecordBolt();
        TrainModelBolt trainModelBolt = new TrainModelBolt(keywords);
        DeciderBolt deciderBolt = new DeciderBolt(keywords);
        DBBolt dbBolt = new DBBolt();

        //bolt for testing
        /*TempBolt tempBolt = new TempBolt();
        TempBolt tempBolt2 = new TempBolt();
        String TEMP_BOLT_ID = "temp bolt";
        String TEMP_BOLT_ID_2 = "temp bolt 2";*/

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SPOUT_ID,spout);


        topologyBuilder.setBolt(FILTER_BOLT_ID, filterBolt,3).shuffleGrouping(SPOUT_ID);

        String allMatchStream = filterBolt.ALL_MATCH_STREAM;
        String someMatchStream = filterBolt.SOME_MATCH_STREAM;
        BoltDeclarer bdRecordBolt = topologyBuilder.setBolt(RECORD_BOLT_ID, recordBolt);
        bdRecordBolt.allGrouping(FILTER_BOLT_ID,allMatchStream);
        bdRecordBolt.allGrouping(FILTER_BOLT_ID,someMatchStream);

        String allTextSent = recordBolt.ALL_TEXT_SENT;
        String positiveStream = recordBolt.POSITIVE_STREAM;
        //topologyBuilder.setBolt(TEMP_BOLT_ID,tempBolt,1).globalGrouping(RECORD_BOLT_ID,positiveStream);
        //topologyBuilder.setBolt(TEMP_BOLT_ID_2,tempBolt2,1).globalGrouping(RECORD_BOLT_ID,allTextSent);
        topologyBuilder.setBolt(MODEL_BOLT_ID, trainModelBolt).globalGrouping(RECORD_BOLT_ID, allTextSent);

        String seedStream = trainModelBolt.SEED_STREAM_ID;
        BoltDeclarer deciderBoltDeclarer = topologyBuilder.setBolt(DECIDER_BOLT_ID, deciderBolt,3);
        deciderBoltDeclarer.allGrouping(FILTER_BOLT_ID,someMatchStream);
        deciderBoltDeclarer.allGrouping(MODEL_BOLT_ID,seedStream);


        BoltDeclarer dbBoltDeclarer = topologyBuilder.setBolt(DB_BOLT_ID, dbBolt);
        String onTopicStream = deciderBolt.ON_TOPIC_STREAM;
        dbBoltDeclarer.allGrouping(RECORD_BOLT_ID, positiveStream);
        dbBoltDeclarer.allGrouping(DECIDER_BOLT_ID,onTopicStream);

        Config config = new Config();
        config.put("dir", dirRawPost);
        config.put("dirText4Model",dirText4Model);
        config.put("dirCandiPost",dirCandiPost);
        config.put("dirTopicTable",dirTopicTable);
        config.put("dirStopWords",dirStopWords);
        config.put("dirOutPutModel",dirOutPutModel);
        //config.setDebug(true);
        //config.setNumWorkers(1);
        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("Getting-Started-Topology",config,topologyBuilder.createTopology());

        Utils.sleep(30000L);  //1800000L
        localCluster.killTopology("Getting-Started-Topology");
        localCluster.shutdown();

    }


}
