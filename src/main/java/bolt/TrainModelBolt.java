package bolt;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.CollectionSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.LineSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import type.TumblrPost;

import java.io.File;
import java.math.BigDecimal;
import java.util.*;

public class TrainModelBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static String[] keywords;
    public final String SEED_STREAM_ID = "seeds";

    private String dirInputModel;
    private static boolean flag = false;  //determine whether modeling completed
    private static int round =0;

    private static Collection<String> textCache;
    private int limit;
    private Word2Vec vec;
    private HashMap<String, Double> seedList;
    public static List<String> newKeywords = new ArrayList<>();
    private boolean flagNotFound = true;

    public TrainModelBolt(String[] keywords) {
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.dirInputModel = (String) map.get("dirInputModel");
        this.limit = this.keywords.length*15;
        this.textCache = new ArrayList<>();
        this.seedList = new HashMap<>();
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("ModelBolt Round: "+round+"  "+Arrays.asList(this.keywords));
        System.out.println("ModelBolt flag: "+flag+" now cache size is "+textCache.size());
        if (flag){
            Utils.sleep(1000);
            return;
        }
        if(textCache.size()<=limit){
            String text = tuple.getStringByField("text");
            textCache.add(text);
            return;
        }else{
            vec = updateModel();
            System.out.println("Searching the nearest words to the given keyword");
            //log.info("Searching the nearest words to the given keyword");
            for(String kw:keywords){
                if(vec.hasWord(kw)){
                    List<String> seeds = (List<String>) vec.wordsNearest(kw,10);
                    //showSimilarity(kw, vec, vec.wordsNearest(kw,10));
                    System.out.println(seeds);
                    this.collector.emit(SEED_STREAM_ID,new Values(kw,seeds));
                }else{
                    System.out.println("Vector space does not contain word ["+kw+"], please train model first");
                }

            }
            newKeywords = queryExpansion(vec);

            Utils.sleep(15000L);
            this.round = addKeywords(newKeywords);

            flag = true;
        }

    }

    private int addKeywords(List<String> newKeywords) {
        int length = this.keywords.length+newKeywords.size();
        String[] tem = new String[length];
        for(int i=0;i<this.keywords.length;i++){
            tem[i] = this.keywords[i];
        }
        for(int j=0;j<this.newKeywords.size();j++){
            tem[j+this.keywords.length] = this.newKeywords.get(j);
        }
        this.keywords = tem;
        this.limit = this.keywords.length*15;
        this.textCache = new ArrayList<>();
        this.newKeywords = new ArrayList<>();
        round++;
        return round;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SEED_STREAM_ID, new Fields("keyword","seeds"));
    }

    private List<String> queryExpansion(Word2Vec vec) {
        this.seedList = new HashMap<>();
        List<String> kwList = Arrays.asList(this.keywords);
        for(String kw:kwList){
            List<String> seeds = (List<String>) vec.wordsNearest(kw, 10);//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            for(String seed:seeds){
                String stemmedSeed = "";
                if(seed.endsWith("s")){
                    int len = seed.length()-1;
                    stemmedSeed = seed.substring(0,len);
                    System.out.println(stemmedSeed);
                }
                if((!seedList.containsKey(seed))&&(!kwList.contains(seed))
                        &&(!kwList.contains(stemmedSeed))&&(!kwList.contains(seed+"s"))){
                    double score = scoreSeed(seed, kwList, vec);
                    seedList.put(seed,score);
                }
            }
        }

        List<Map.Entry<String, Double>> seedRank = new ArrayList<>(seedList.entrySet());
        // rank seedList
        Collections.sort(seedRank, (o1, o2) ->
                (BigDecimal.valueOf(o2.getValue()).compareTo(BigDecimal.valueOf(o1.getValue()))));

        // show rank result
        for (int i = 0; i < seedRank.size(); i++) {

            String id = seedRank.get(i).getKey();
            double score = seedRank.get(i).getValue();
            System.out.println(id + "  "+score);
            if(i<3){
                newKeywords.add(id);
            }
        }
        return newKeywords;

    }

    private static double scoreSeed(String seed, List<String> keywords, Word2Vec vec) {
        System.out.print(seed+" [");
        BigDecimal score = new BigDecimal(1.0);
        BigDecimal ten = new BigDecimal(10.0);
        for(String kw:keywords){
            BigDecimal sim = new BigDecimal(vec.similarity(kw,seed)).multiply(ten);
            System.out.print(sim+", ");
            score = score.multiply(sim);
        }
        System.out.println("]");
        return score.doubleValue();
    }

    private Word2Vec updateModel() {
        this.vec = WordVectorSerializer.readWord2VecModel(dirInputModel);

        SentenceIterator iterator = new CollectionSentenceIterator(textCache);
        iterator.setPreProcessor((SentencePreProcessor) s -> {
            if(StringUtils.isNotBlank(s)){
                return s;
            } else{
                return "";
            }
        });

        TokenizerFactory tFactory = new DefaultTokenizerFactory();
        tFactory.setTokenPreProcessor(new CommonPreprocessor());

        System.out.println("Updating Model...");
        vec.setTokenizerFactory(tFactory);
        vec.setSentenceIterator(iterator);

        System.out.println("Fitting Model...");
        //log.info("Fitting Model...");
        vec.fit();
        return vec;
    }

    private Collection<String> showSimilarity(String kw, Word2Vec vec, Collection<String> seeds) {
        System.out.println("*******for "+kw+" *********");
        for(String seed:seeds){
            System.out.print(seed);
            for(String k:keywords){
                if(seed.equalsIgnoreCase(k)){
                    System.out.print("      "+vec.similarity(k,seed));
                    seeds.remove(seed);
                }
            }
            System.out.println();
        }
        return seeds;
    }

    public String[] getKeywords() {
        return this.keywords;
    }

    public int getRound() {
        return this.round;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }
}

