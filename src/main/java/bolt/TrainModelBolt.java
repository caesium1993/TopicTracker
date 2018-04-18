package bolt;

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
import org.deeplearning4j.text.sentenceiterator.LineSentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import javax.rmi.CORBA.Util;
import java.io.*;
import java.util.*;

public class TrainModelBolt extends BaseRichBolt {
    private OutputCollector collector;
    private String[] keywords;
    public final String ALL_TEXT_SENT = "all text sent";
    public final String SEED_STREAM_ID = "seeds";

    private String dirText4Model;
    private String dirStopWords;
    private String dirOutPutModel;
    private List<String> stopList;
    private boolean flag = false;

    public TrainModelBolt(String[] keywords) {
        this.keywords = keywords;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.dirStopWords = (String) map.get("dirStopWords");
        this.dirOutPutModel = (String) map.get("dirOutPutModel");

        this.stopList = new ArrayList<>();

        try {
            stopList = getStopWordList(dirStopWords);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (flag){
            Utils.sleep(1000);
            return;
        }
        if(tuple.getSourceStreamId().equals(ALL_TEXT_SENT)){
            this.dirText4Model = tuple.getStringByField("dir text4model");

            File file = new File(this.dirText4Model);
            SentenceIterator iterator = new LineSentenceIterator(file);
            iterator.setPreProcessor(new SentencePreProcessor() {
                public String preProcess(String s) {
                    return s.toLowerCase().replaceAll("[^_0-9a-zA-Z ]","").
                            replaceAll("__+","").
                            replaceAll("[ ]+"," ");
                }
            });

            TokenizerFactory tFactory = new DefaultTokenizerFactory();
            tFactory.setTokenPreProcessor(new CommonPreprocessor());

            //ocabCache<VocabWord> cache = new AbstractCache<>();
        /*WeightLookupTable<VocabWord> table = new InMemoryLookupTable.Builder<VocabWord>()
                .vectorLength(100)
                .useAdaGrad(false)
                .cache(cache).build();*/

            //log.info("Building Model...");
            System.out.println("Building Model...");
            Word2Vec vec = new Word2Vec.Builder()
                    .minWordFrequency(5)
                    //.allowParallelTokenization(true)
                    .layerSize(200)  //dimensions in the feature space
                    .seed(42)
                    .windowSize(5)
                    .iterations(5)
                    .iterate(iterator)
                    //.lookupTable(table)
                    //.vocabCache(cache)
                    .stopWords(stopList)
                    .tokenizerFactory(tFactory).build();

            System.out.println("Fitting Model...");
            //log.info("Fitting Model...");
            vec.fit();

            System.out.println("Saving Model...");
            //log.info("Saving Model...");
            WordVectorSerializer.writeWord2VecModel(vec,dirOutPutModel);

            System.out.println("Searching the nearest words to the given keyword");
            //log.info("Searching the nearest words to the given keyword");
            for(String kw:keywords){
                System.out.println("********"+"for "+kw+"********");
                Collection<String> seeds = vec.wordsNearest(kw,10);
                //vec.wordsNearest("melbourne",10);
                //List<String> results = vec.similarWordsInVocabTo("melbourne",0.5);
                System.out.println(seeds);
                for(String s:seeds){
                    double sim  = vec.similarity(s,kw);
                    System.out.println(s+"  "+sim);
                    this.collector.emit(SEED_STREAM_ID, new Values(kw,s));
                }
            }

            flag = true;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(SEED_STREAM_ID, new Fields("keywords","seeds"));
    }

    public List<String> getStopWordList(String dir) throws IOException {
        File file = new File(dir);
        BufferedReader br = new BufferedReader(new FileReader(file));

        String str;
        while((str=br.readLine())!=null){
            if(str.trim().equals("")){
                continue;
            } else {
                this.stopList.add(str);
            }
        }
        return this.stopList;
    }
}
