import bolt.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import scheduledTask.RequestBlogNameTask;
import scheduledTask.RequestPostTask;
import scheduledTask.RequestTaggedPostTask;
import spout.TumblrSpout;
import type.TumblrPost;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final String MODEL_BOLT_ID = "train model bolt";
    private static final String DECIDER_BOLT_ID = "decider bolt";
    private static final String DB_BOLT_ID = "DB bolt";

    public static Hashtable<Long, TumblrPost> posts = new Hashtable<>();
    public static String[] keywords = null;

    public static String dirRawPost ="data/raw_posts.json";
    private static String dirInputModel = "data/pre_model_20_05_2018.txt";
    //private static String dirInputModel = "E://data/storm/pre_model_29_04_2018.txt";

    private static TopologyBuilder topologyBuilder;
    private static LocalCluster localCluster;
    private static int round = 0;

    public static void main(String[] args) throws InterruptedException, IOException {
        //args are the given input keywords
        keywords = toLowCase(args);

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
        taggedPostTask.cleanRawData();
        long tagDelay = 5;
        pool.scheduleWithFixedDelay(taggedPostTask, 0, tagDelay, TimeUnit.MINUTES);

        /**
         * get the list of all following blogs
         */
       while(flag4BlogNameTask==false){
            Thread.sleep(500);
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
        long delay = 2;
        pool.scheduleWithFixedDelay(getPostTask, 0, delay, TimeUnit.HOURS);

        Thread.sleep(1000L);

        /**
         * set up apache storm
         */
        //initialize spout and bolts
        //set and submit topology
        System.out.println("I am continuing now");
        topologyBuilder = new TopologyBuilder();
        localCluster = new LocalCluster();

        TumblrSpout spout = new TumblrSpout();
        FilterBolt filterBolt = new FilterBolt(keywords);
        //RecordBolt recordBolt = new RecordBolt();
        TrainModelBolt trainModelBolt = new TrainModelBolt(keywords);
        DeciderBolt deciderBolt = new DeciderBolt(keywords);
        DBBolt dbBolt = new DBBolt(keywords);

        //bolt for testing
        /*TempBolt tempBolt = new TempBolt();
        TempBolt tempBolt2 = new TempBolt();
        String TEMP_BOLT_ID = "temp bolt";
        String TEMP_BOLT_ID_2 = "temp bolt 2";*/

        topologyBuilder.setSpout(SPOUT_ID,spout);


        topologyBuilder.setBolt(FILTER_BOLT_ID, filterBolt,3).shuffleGrouping(SPOUT_ID);

        String allMatchStream = filterBolt.ALL_MATCH_STREAM;
        String someMatchStream = filterBolt.SOME_MATCH_STREAM;
        BoltDeclarer bdTempBolt = topologyBuilder.setBolt(MODEL_BOLT_ID, trainModelBolt);
        bdTempBolt.allGrouping(FILTER_BOLT_ID,allMatchStream);
        bdTempBolt.allGrouping(FILTER_BOLT_ID,someMatchStream);

        String seedStream = trainModelBolt.SEED_STREAM_ID;


        BoltDeclarer deciderBoltDeclarer = topologyBuilder.setBolt(DECIDER_BOLT_ID, deciderBolt);
        deciderBoltDeclarer.allGrouping(FILTER_BOLT_ID,someMatchStream);
        deciderBoltDeclarer.allGrouping(MODEL_BOLT_ID,seedStream);


        String onTopicStream = deciderBolt.ON_TOPIC_STREAM;
        String offTopicStream = deciderBolt.OFF_TOPIC_STREAM;
        BoltDeclarer dbBoltDeclarer = topologyBuilder.setBolt(DB_BOLT_ID,dbBolt);
        dbBoltDeclarer.allGrouping(FILTER_BOLT_ID, allMatchStream);
        dbBoltDeclarer.allGrouping(DECIDER_BOLT_ID, onTopicStream);
        dbBoltDeclarer.allGrouping(DECIDER_BOLT_ID, offTopicStream);
        /*BoltDeclarer dbBoltDeclarer = topologyBuilder.setBolt(DB_BOLT_ID, dbBolt);
        String onTopicStream = deciderBolt.ON_TOPIC_STREAM;
        dbBoltDeclarer.allGrouping(RECORD_BOLT_ID, positiveStream);
        dbBoltDeclarer.allGrouping(DECIDER_BOLT_ID,onTopicStream);*/

        Config config = new Config();
        config.put("dirRawPost", dirRawPost);
        config.put("dirInputModel",dirInputModel);


        localCluster.submitTopology("Getting-Started-Topology",config,topologyBuilder.createTopology());

        /**
         * loop: while keywords size changed and DBBlot flag changed, begin a new iteration
         * new iteration: set new keywords to query tasks
         *                set new keywords to spout and bolts
         *                set BDbolt flag to false
         */
        //String[] test = {"au","american","america","trump","usa politics","usa news"};
        int t = 3;
        //System.out.println("test 1");
        while(round<4) {
            Thread.sleep(1000L);
            //System.out.println("test 2");
            int mark = trainModelBolt.getRound();
            if (mark > round) {
                keywords = trainModelBolt.getKeywords();
                System.out.println("This is main: " + Arrays.asList(keywords));
                Thread.sleep(7000L);
                taggedPostTask.setKeywords(keywords);
                getPostTask.setKeywords(keywords);
                round++;
                spout.setRound(round);
                filterBolt.setKeywords(keywords);
                trainModelBolt.setFlag(false);
                deciderBolt.setKeywords(keywords);
                dbBolt.setKeywords(keywords);
                dbBolt.setRound(round);
                //decider
                //dbBolt
            }
        }

        localCluster.killTopology("Getting-Started-Topology");
        localCluster.shutdown();



    }

    private static String[] addNewKW(String[] keywords, String s, String s1, String s2) {
        String[] res = new String[keywords.length+3];
        for(int i=0;i<keywords.length;i++){
            res[i]=keywords[i];
        }
        int j = keywords.length;
        res[j] = s;
        res[j+1] = s1;
        res[j+2] = s2;

        return res;
    }

    private static String[] toLowCase(String[] args) {
        for(int i=0;i<args.length;i++){
            args[i] = args[i].toLowerCase().trim();
        }

        return args;
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

    }


}
