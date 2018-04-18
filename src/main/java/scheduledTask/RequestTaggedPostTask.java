package scheduledTask;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.types.*;
import type.TumblrPost;
import utils.FileWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class RequestTaggedPostTask {
    private String consumer_key;
    private String consumer_secret;
    private String access_token;
    private String token_secret;
    private String[] keywords;
    private Hashtable<Long, TumblrPost> taggedPosts = new Hashtable<>();

    public String dir = "E://data/post_1.txt";
    private FileWriter fw;
    private Gson gson = new GsonBuilder().create();

    public RequestTaggedPostTask(String consumer_key, String consumer_secret, String access_token,
                                 String token_secret, String[] keywords) {
        this.consumer_key = consumer_key;
        this.consumer_secret = consumer_secret;
        this.access_token = access_token;
        this.token_secret = token_secret;
        this.keywords = keywords;

        try {
            this.fw = new FileWriter(dir);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Hashtable<Long, TumblrPost> requestByTag() throws IOException {
        if (this.keywords == null) {
            return null;
        }
        JumblrClient client = new JumblrClient(this.consumer_key, this.consumer_secret,
                this.access_token, this.token_secret);
        Map<String, Object> param = new HashMap<>();
        param.put("filter", "text");
        for (String kw : this.keywords) {
            List<Post> result = client.tagged(kw, param);
            this.taggedPosts.putAll(getPostsForGivenBlogs(result, this.taggedPosts));
        }
        fw.close();
        return this.taggedPosts;
    }

    /**
     * get the details of the posts in the given format
     *
     * @param returnedPosts
     */
    /**
     * get the details of the posts in the given format
     * @param returnedPosts
     */
    public Hashtable<Long, TumblrPost> getPostsForGivenBlogs(List<Post> returnedPosts,
                                                             Hashtable<Long, TumblrPost> posts) throws IOException {
        // TODO Auto-generated method stub
        for (Post item : returnedPosts) {
            if (posts.containsKey(item.getId()) == false) {
                String type = item.getType().toString();
                switch (type) {
                    case "text":
                        TextPost tPost = (TextPost) item;
                        String text = tPost.getTitle().trim().replaceAll("[\r\n]+", "") + " "
                                + tPost.getBody().trim().replaceAll("[\r\n]+", "");
                        List<String> tags = item.getTags();
                        //Long id, String type, String text, String date, String blog_name, List<String> tags
                        TumblrPost p = new TumblrPost(item.getId(), item.getType(), text.trim(), item.getDateGMT(),
                                item.getBlogName(), tags);
                        posts.put(item.getId(), p);
                        fw.writeSingleLine(gson.toJson(p));
                        //System.out.println(text);
                        //System.out.println("["+i+"]"+tPost.getBlogName()+": "+tPost.getBody().trim());
                        //System.out.println(tPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "photo":
                        PhotoPost pPost = (PhotoPost) item;
                        if (pPost.getCaption() != null) {
                            String text1 = pPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags1 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p1 = new TumblrPost(item.getId(), item.getType(), text1, item.getDateGMT(),
                                    item.getBlogName(), tags1);
                            posts.put(item.getId(), p1);
                            fw.writeSingleLine(gson.toJson(p1));
                            //System.out.println(text1);
                        }
                        //System.out.println("["+i+"]"+pPost.getBlogName()+": "+pPost.getCaption().trim());
                        //System.out.println(pPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "quote":
                        QuotePost qPost = (QuotePost) item;
                        if (qPost.getText() != null) {
                            String text2 = qPost.getText().trim().replaceAll("[\r\n]+", "") + " " +
                                    qPost.getSource().trim().replaceAll("[\r\n]+", "");
                            List<String> tags2 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p2 = new TumblrPost(item.getId(), item.getType(), text2.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags2);
                            posts.put(item.getId(), p2);
                            fw.writeSingleLine(gson.toJson(p2));
                            //System.out.println(text2);
                        }
                        //System.out.println("["+i+"]"+qPost.getBlogName()+": "+qPost.getText().trim()+" Source: "+qPost.getSource());
                        //System.out.println(qPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "link":
                        LinkPost lPost = (LinkPost) item;
                        if (lPost.getTitle() != null || lPost.getDescription() != null) {
                            String text3 = lPost.getTitle().trim().replaceAll("[\r\n]+", "")
                                    + lPost.getDescription().trim().replaceAll("[\r\n]+", "");
                            List<String> tags3 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p3 = new TumblrPost(item.getId(), item.getType(), text3.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags3);
                            posts.put(item.getId(), p3);
                            fw.writeSingleLine(gson.toJson(p3));
                            //System.out.println(text3);
                        }
                        //System.out.println("["+i+"]"+lPost.getBlogName()+": "+lPost.getTitle()+ "Description: "+lPost.getDescription().trim());
                        //System.out.println(lPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "chat":
                        ChatPost cPost = (ChatPost) item;
                        if (cPost.getBody() != null || cPost.getTitle() != null) {
                            String text4 = cPost.getBody().trim().replaceAll("[\r\n]+", "")
                                    + " " + cPost.getTitle().trim().replaceAll("[\r\n]+", "");
                            List<String> tags4 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p4 = new TumblrPost(item.getId(), item.getType(), text4.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags4);
                            posts.put(item.getId(), p4);
                            fw.writeSingleLine(gson.toJson(p4));
                            //System.out.println(text4);
                        }
                        //System.out.println("["+i+"]"+cPost.getBlogName()+": "+cPost.getBody().trim());
                        //System.out.println(cPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "audio":
                        AudioPost aPost = (AudioPost) item;
                        if (aPost.getCaption() != null) {
                            String text5 = aPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags5 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p5 = new TumblrPost(item.getId(), item.getType(), text5.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags5);
                            posts.put(item.getId(), p5);
                            fw.writeSingleLine(gson.toJson(p5));
                            //System.out.println(text5);
                        }
                        //System.out.println("["+i+"]"+aPost.getBlogName()+": "+aPost.getAlbumName()+"-"+aPost.getArtistName()
                        //+": "+aPost.getCaption().trim());
                        //System.out.println(aPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "video":
                        VideoPost vPost = (VideoPost) item;
                        if (vPost.getCaption() != null) {
                            String text6 = vPost.getCaption().trim().replaceAll("[\r\n]+", "");
                            List<String> tags6 = item.getTags();
                            //Long id, String type, String text, String date, String blog_name, List<String> tags
                            TumblrPost p6 = new TumblrPost(item.getId(), item.getType(), text6, item.getDateGMT(),
                                    item.getBlogName(), tags6);
                            posts.put(item.getId(), p6);
                            fw.writeSingleLine(gson.toJson(p6));
                            //System.out.println(text6);
                        }
                        //System.out.println("["+i+"]"+vPost.getBlogName()+": "+vPost.getCaption().trim());
                        //System.out.println(vPost.toString()+"  "+item.getDateGMT());
                        break;
                    case "answer":
                        AnswerPost ansPost = (AnswerPost) item;
                        if (ansPost.getQuestion() != null || ansPost.getAnswer() != null) {
                            String text7 = ansPost.getQuestion().trim().replaceAll("[\r\n]+", "")
                                    + " " + ansPost.getAnswer().trim().replaceAll("[\r\n]+", "");
                            List<String> tags7 = item.getTags();
                            TumblrPost p7 = new TumblrPost(item.getId(), item.getType(), text7.trim(), item.getDateGMT(),
                                    item.getBlogName(), tags7);
                            posts.put(item.getId(), p7);
                            fw.writeSingleLine(gson.toJson(p7));
                            //System.out.println(text7);
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
}