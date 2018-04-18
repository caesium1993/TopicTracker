package scheduledTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import com.tumblr.jumblr.JumblrClient;
import com.tumblr.jumblr.types.AnswerPost;
import com.tumblr.jumblr.types.AudioPost;
import com.tumblr.jumblr.types.Blog;
import com.tumblr.jumblr.types.ChatPost;
import com.tumblr.jumblr.types.LinkPost;
import com.tumblr.jumblr.types.PhotoPost;
import com.tumblr.jumblr.types.Post;
import com.tumblr.jumblr.types.QuotePost;
import com.tumblr.jumblr.types.TextPost;
import com.tumblr.jumblr.types.User;
import com.tumblr.jumblr.types.VideoPost;

/**
 * This class is to request Tumblr posts and their notes
 * @author Minjie ZHU <minjiez@student.unimeb.edu.au>
 *
 */
public class RequestBlogNameTask implements Callable {

    private String consumer_key;
    private String consumer_secret;
    private String access_token;
    private String token_secret;
    protected ArrayList<String> blogLists = new ArrayList<>();
    //private ArrayList<Post> returnedPosts;
    //public Hashtable<Long, Post> posts;

    public RequestBlogNameTask(String consumer_key, String consumer_secret, String access_token, String token_secret) {
        this.consumer_key = consumer_key;
        this.consumer_secret = consumer_secret;
        this.access_token = access_token;
        this.token_secret = token_secret;
    }

    @Override
    public ArrayList<String> call() throws Exception {
        JumblrClient client = new JumblrClient(this.consumer_key, this.consumer_secret, this.access_token,
                this.token_secret);

        User user = client.user();
        int fCount = user.getFollowingCount(); // following count
        System.out.println("User name: "+user.getName());
        System.out.println("User Following: "+fCount);

        /**
         * read the following blogs of given user, return 20 per time
         */
        int iteration; // number of iteration
        int limit = 20; // return 20 per time
        int remainder = 0;//number of last iteration
        if(fCount%limit==0){
            iteration=fCount/limit;
        } else {
            iteration=fCount/limit+1;
            remainder = fCount%limit;
        }

        for(int i=0; i<iteration;i++){
            if (i==(iteration-1)&&remainder!=0){
                limit = remainder;
            }
            Map<String, Object> param = new HashMap<>();
            param.put("limit", limit);
            param.put("offset", i*iteration);
            List<Blog> followingBlogs = client.userFollowing(param);
            blogLists.addAll(getBlogNamesForFollowing(followingBlogs));
            //System.out.println("size: "+blogLists.size());
        }
        return blogLists;
    }

    /**
     * list the names of the following blogs
     * @param fBlogs list of following blogs
     * @return list of the following blognames
     */
    private static ArrayList<String> getBlogNamesForFollowing(List<Blog> fBlogs) {
        // TODO Auto-generated method stub
        ArrayList<String> blogLists = new ArrayList<String>();
        for(Blog blog:fBlogs){
            blogLists.add(blog.getName()+".tumblr.com");
            //System.out.println(blog.getName());
        }
        return blogLists;
    }


}
