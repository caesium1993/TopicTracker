package dedup;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import type.TumblrPost;
import utils.FileWriter;

import java.io.*;
import java.util.HashSet;

public class Deduper {

    public static void main(String[] args) throws IOException {
        File fileInput = new File("E://data/post_2.json");
        FileWriter fw = new FileWriter("E://data/storm/tem_post.json",false);
        FileWriter fwId = new FileWriter("E://data/storm/tem_id.txt",false);

        Gson gson = new GsonBuilder().create();
        HashSet<Long> idSet = new HashSet<>();

        FileInputStream in = new FileInputStream(fileInput);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        int count = 0;
        String str = null;
        while((str = br.readLine())!=null){
            if(StringUtils.isNotBlank(str)){
                TumblrPost post = gson.fromJson(str, TumblrPost.class);
                if(idSet.add(post.getId())){
                    fwId.writeSingleLine(post.getId().toString());
                    fw.writeSingleLine(str);
                } else{
                    count++;
                    System.out.println(count+": "+str);
                }
            }

        }

        br.close();
        fwId.close();
        fw.close();
        in.close();

    }
}
