package scheduledTask;

import com.google.gson.Gson;
import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.types.*;
import org.apache.commons.lang3.StringUtils;
import type.TumblrPost;
import utils.FileWriter;

import java.io.*;
import java.util.*;

public class RequestPostTask implements Runnable {

    private String consumer_key;
    private String consumer_secret;
    private String access_token;
    private String token_secret;
    public ArrayList<String> blogLists;
    public String[] keywords;

    private String dir = "E://data/post_2.json";
    private String dirRawPosts = "E://data/storm/raw_posts.json";
    private String dirId = "E://data/post_id.txt";

    public String getDir() {
        return dirRawPosts;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public HashSet<Long> posts = new HashSet<>();
    private Gson gson = new Gson();

    public RequestPostTask(String consumer_key, String consumer_secret, String access_token, String token_secret,
                           ArrayList<String> blogLists, String[] keywords) throws FileNotFoundException {
        this.consumer_key = consumer_key;
        this.consumer_secret = consumer_secret;
        this.access_token = access_token;
        this.token_secret = token_secret;
        this.blogLists = blogLists;
        this.keywords = keywords;
    }

    public  boolean cleanRawData(){
        boolean flag1 = false;
        //boolean flag2 = false;
        File rawFile = new File(dirRawPosts);
        File idFile = new File(dirId);

        if(rawFile.exists()){
            flag1 = rawFile.delete();
        }

        /*if(idFile.exists()){
            flag2 = idFile.delete();
        }*/

        if(flag1){
            return true;
        } else{
            return false;
        }
    }

    @Override
    public void run() {
        System.out.println(showKeywords());

        File idFile = new File(dirId);
        /**
         * if it has queried before
         */
        if(idFile.exists()){
            try {
                BufferedReader br = new BufferedReader(new FileReader(idFile));
                String line= br.readLine();
                while(line!=null){
                    this.posts.add(Long.parseLong(line));
                    line = br.readLine();
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        /**
         * first time
         */

        JumblrClient client = new JumblrClient(this.consumer_key, this.consumer_secret, this.access_token,
                this.token_secret);
        System.out.println("I am running  The post size is "+this.posts.size());

        /**
         * read the posts of the given blogs
         */
        Map<String, Object> param = new HashMap<>();
        int limit = 20; // number of returned posts each time
        param.put("limit", limit);
        //param.put("reblog_info", true);
        param.put("filter", "text");

        int i=0;
        for(String blogName:this.blogLists){
            System.out.println("Round"+i+"this is for "+blogName+" The size is "+this.posts.size());
            try {
                FileWriter fw = new FileWriter(dir); // just for pre-modelling!!!!!!!
                FileWriter fw1 = new FileWriter(dirRawPosts); // FileWriter for npl
                this.posts = getPostsForGivenBlogs(client.blogPosts(blogName, param), this.posts, fw, fw1);
                fw1.close();
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            i++;
        }

        try {
            FileWriter fw2 = new FileWriter(dirId, false);
            fw2.write(dirId,this.posts);
            fw2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * get the details of the posts in the given format
     * @param returnedPosts
     */
    public HashSet<Long> getPostsForGivenBlogs(List<Post> returnedPosts, HashSet<Long> posts,
                                               FileWriter fw, FileWriter fw1) throws IOException {
        // TODO Auto-generated method stub
        for (Post item : returnedPosts) {
            if (posts.add(item.getId())) {
                String type = item.getType().toString();
                switch (type) {
                    case "text":
                        TextPost tPost = (TextPost) item;
                        if(StringUtils.isNotBlank(tPost.getBody())||StringUtils.isNotBlank(tPost.getTitle())){
                            String text = tPost.getTitle().trim().replaceAll("[\r\n]+", "") + " "
                                    + tPost.getBody().trim().replaceAll("[\r\n]+", "");
                            List<String> tags = item.getTags();

                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p = new TumblrPost(item.getId(), item.getType(), text.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags);
                            fw.writeSingleLine(gson.toJson(p));
                            /**********if the post contains any given keyword**********/
                            if(filterPost(text, tags)){
                                fw1.writeSingleLine(gson.toJson(p));
                            }
                            System.out.println(text);
                        }

                        //System.out.println("["+i+"]"+tPost.getBlogName()+": "+tPost.getBody().trim());
                        //System.out.println(tPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "photo":
                        PhotoPost pPost = (PhotoPost) item;
                        if (StringUtils.isNotBlank(pPost.getCaption())) {
                            String text1 = pPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags1 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p1 = new TumblrPost(item.getId(), item.getType(), text1, item.getDateGMT(),
                                    item.getBlogName(), tags1);
                            fw.writeSingleLine(gson.toJson(p1));
                            if(filterPost(text1, tags1)){
                                fw1.writeSingleLine(gson.toJson(p1));
                            }
                            System.out.println(text1);
                        }
                        //System.out.println("["+i+"]"+pPost.getBlogName()+": "+pPost.getCaption().trim());
                        //System.out.println(pPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "quote":
                        QuotePost qPost = (QuotePost) item;
                        if (StringUtils.isNotBlank(qPost.getText())) {
                            String text2 = qPost.getText().trim().replaceAll("[\r\n]+", "") + " " +
                                    qPost.getSource().trim().replaceAll("[\r\n]+", "");
                            List<String> tags2 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p2 = new TumblrPost(item.getId(), item.getType(), text2.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags2);
                            fw.writeSingleLine(gson.toJson(p2));
                            if(filterPost(text2, tags2)){
                                fw1.writeSingleLine(gson.toJson(p2));
                            }
                            System.out.println(text2);
                        }
                        //System.out.println("["+i+"]"+qPost.getBlogName()+": "+qPost.getText().trim()+" Source: "+qPost.getSource());
                        //System.out.println(qPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "link":
                        LinkPost lPost = (LinkPost) item;
                        if (StringUtils.isNotBlank(lPost.getTitle())  || StringUtils.isNotBlank(lPost.getDescription())) {
                            String text3 = lPost.getTitle().trim().replaceAll("[\r\n]+", "")
                                    + lPost.getDescription().trim().replaceAll("[\r\n]+", "");
                            List<String> tags3 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p3 = new TumblrPost(item.getId(), item.getType(), text3.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags3);
                            fw.writeSingleLine(gson.toJson(p3));
                            if(filterPost(text3, tags3)){
                                fw1.writeSingleLine(gson.toJson(p3));
                            }
                            System.out.println(text3);
                        }
                        //System.out.println("["+i+"]"+lPost.getBlogName()+": "+lPost.getTitle()+ "Description: "+lPost.getDescription().trim());
                        //System.out.println(lPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "chat":
                        ChatPost cPost = (ChatPost) item;
                        if (StringUtils.isNotBlank(cPost.getBody()) || StringUtils.isNotBlank(cPost.getTitle())) {
                            String text4 = cPost.getBody().trim().replaceAll("[\r\n]+", "")
                                    + " " + cPost.getTitle().trim().replaceAll("[\r\n]+", "");
                            List<String> tags4 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p4 = new TumblrPost(item.getId(), item.getType(), text4.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags4);
                            fw.writeSingleLine(gson.toJson(p4));
                            if(filterPost(text4, tags4)){
                                fw1.writeSingleLine(gson.toJson(p4));
                            }
                            System.out.println(text4);
                        }
                        //System.out.println("["+i+"]"+cPost.getBlogName()+": "+cPost.getBody().trim());
                        //System.out.println(cPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "audio":
                        AudioPost aPost = (AudioPost) item;
                        if (StringUtils.isNotBlank(aPost.getCaption())) {
                            String text5 = aPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags5 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p5 = new TumblrPost(item.getId(), item.getType(), text5.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags5);
                            fw.writeSingleLine(gson.toJson(p5));
                            if(filterPost(text5, tags5)){
                                fw1.writeSingleLine(gson.toJson(p5));
                            }
                            System.out.println(text5);
                        }
                        //System.out.println("["+i+"]"+aPost.getBlogName()+": "+aPost.getAlbumName()+"-"+aPost.getArtistName()
                        //+": "+aPost.getCaption().trim());
                        //System.out.println(aPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "video":
                        VideoPost vPost = (VideoPost) item;
                        if (StringUtils.isNotBlank(vPost.getCaption())) {
                            String text6 = vPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags6 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p6 = new TumblrPost(item.getId(), item.getType(), text6, item.getDateGMT(),
                                    item.getBlogName(), tags6);
                            fw.writeSingleLine(gson.toJson(p6));
                            if(filterPost(text6, tags6)){
                                fw1.writeSingleLine(gson.toJson(p6));
                            }
                            System.out.println(text6);
                        }
                        //System.out.println("["+i+"]"+vPost.getBlogName()+": "+vPost.getCaption().trim());
                        //System.out.println(vPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "answer":
                        AnswerPost ansPost = (AnswerPost) item;
                        if (StringUtils.isNotBlank(ansPost.getQuestion())||StringUtils.isNotBlank(ansPost.getAnswer())) {
                            String text7 = ansPost.getQuestion().trim().replaceAll("[\r\n]+", "")
                                    + " " + ansPost.getAnswer().trim().replaceAll("[\r\n]+", "");
                            List<String> tags7 = item.getTags();
                            TumblrPost p7 = new TumblrPost(item.getId(), item.getType(), text7.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags7);
                            fw.writeSingleLine(gson.toJson(p7));
                            if(filterPost(text7, tags7)){
                                fw1.writeSingleLine(gson.toJson(p7));
                            }
                            System.out.println(text7);
                        }
                        //System.out.println("["+i+"]"+ansPost.getBlogName()+": "+ansPost.getQuestion().trim()+"Answer: "+ansPost.getAnswer().trim());
                        //System.out.println(ansPost.toString()+"  "+item.getDateGMT());
                        break;
                    default:
                        break;
                }
            }

        }
        return posts;
    }

    private boolean filterPost(String text, List<String> tags) {
        boolean flag = false;
        for(String kw:keywords){
            kw = kw.replaceAll("_", " ");
            if(!tags.isEmpty()){
                if(text.toLowerCase().contains(kw)||tags.contains(kw)){
                    flag = true;
                    break;
                }
            } else {
                if(text.toLowerCase().contains(kw)){
                    flag = true;
                    break;
                }
            }

        }
        return flag;

    }

    public void setKeywords(String[] keywords) {
        this.keywords = keywords;
    }

    public String showKeywords() {
        return "RequestPostTask{" +
                "keywords=" + Arrays.toString(this.keywords) +
                '}';
    }
}
