import bolt.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import scheduledTask.RequestBlogNameTask;
import scheduledTask.RequestPostTask;
import scheduledTask.RequestTaggedPostTask;
import spout.TumblrSpout;
import type.TumblrPost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.concurrent.*;


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

    public static String dirRawPost ="E://data/storm/raw_posts.json";
    //public static String dirText4Model = "E://data/storm/text_model.txt";
    //public static String dirCandiPost = "E://data/storm/candidate_post.json";
    public static String dirStopWords = "E://data/stopList.txt";
    //public static String dirOutPutModel = "E://data/storm/post2_model.txt";
    public static String dirTopicTable = "E://data/storm/topicTable.json";

    private static TopologyBuilder topologyBuilder;
    private static LocalCluster localCluster;
    private static int round = 2;

    public static void main(String[] args) throws InterruptedException, IOException {
        //args are the given input keywords
        keywords = args;

        /**
         * begin the regular query  (tagged and following blogs)
         */
        System.out.println("now we have "+keywords.length+" keywords");

        ArrayList<String> followingBlogs = new ArrayList<>();

        ScheduledExecutorService pool = Executors.newScheduledThreadPool(5);

        /**
         * request the blog names of all my followings
         */
        RequestBlogNameTask blogNameTask = new RequestBlogNameTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET);
        ScheduledFuture<ArrayList<String>> future = pool.schedule(blogNameTask, 1L, TimeUnit.MILLISECONDS);
        boolean flag4BlogNameTask = false;

        /**
         * periodic request posts by given keywords using tagged method
         */
        RequestTaggedPostTask taggedPostTask = new RequestTaggedPostTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET, keywords);
        long tagDelay = 20;
        pool.scheduleWithFixedDelay(taggedPostTask, 0, tagDelay, TimeUnit.MINUTES);

        /**
         * get the list of all following blogs
         */
        while(flag4BlogNameTask==false){
            Thread.sleep(500L);
            if(future.isDone()){
                flag4BlogNameTask= true;
                try {
                    followingBlogs=future.get();
                    //System.out.println("following blogs size: "+followingBlogs.size());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }

        /*int j=1;
        for(String i:followingBlogs){
            System.out.println("["+j+"]"+i);
            j++;
        }*/
        /**
         * request post by blogPost method
         */
        RequestPostTask getPostTask = new RequestPostTask(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,
                TOKEN_SECRET,followingBlogs,keywords);
        dirRawPost = getPostTask.getDir();
        long delay = 5;
        pool.scheduleWithFixedDelay(getPostTask, 0, delay, TimeUnit.HOURS);

        /**
         * set up apache storm
         */
        //initialize spout and bolts
        //set and submit topology

        /**
         * loop: while keywords size changed and DBBlot flag changed, begin a new iteration
         * new iteration: set new keywords to query tasks
         *                set new keywords to spout and bolts
         *                set BDbolt flag to false
         */

        System.out.println("I am continuing now");
        topologyBuilder = new TopologyBuilder();
        localCluster = new LocalCluster();
        setUpStormTopology(keywords);

        Thread.sleep(10000L);

        localCluster.killTopology("Getting-Started-Topology");
        localCluster.shutdown();

        localCluster = null;
        topologyBuilder = null;

        System.out.println("I am stop");
        /*
        int count;
        while(true){
            Thread.sleep(1000L);  //1800000L
            count = TrainModelBolt.getKeywordListSize();
            System.out.println("count: "+count);
            if(count>0&&(count%2==0)){
                keywords = TrainModelBolt.getNewKeywords();
                Thread.sleep(3000L);
                localCluster.killTopology("Getting-Started-Topology");
                localCluster.shutdown();

                System.out.println("*************************Round "+round+"***********************************");
                round++;
                Thread.sleep(10000L);

                topologyBuilder = new TopologyBuilder();
                localCluster = new LocalCluster();
                setUpStormTopology(keywords);
            }

        }*/



    }

    /**
     * this method regularly query the Tumblr posts for the given keywords using tagged method
     * and blogPost method in order to simulate streaming harvest.
     * @param keywords
     * @throws InterruptedException
     */
    private static void beginQuery(String[] keywords){
    }

    /**
     * set up storm topology
     * @param keywords
     */
    private static void setUpStormTopology(String[] keywords) {
        TumblrSpout spout = new TumblrSpout();
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
        //config.put("dirText4Model",dirText4Model);
        //config.put("dirCandiPost",dirCandiPost);
        config.put("dirTopicTable",dirTopicTable);
        config.put("dirStopWords",dirStopWords);
        //config.put("dirOutPutModel",dirOutPutModel);
        //config.setDebug(true);
        //config.setNumWorkers(1);

        localCluster.submitTopology("Getting-Started-Topology",config,topologyBuilder.createTopology());
    }


}
