package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import type.TumblrPost;
import utils.FileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {
    public static void main(String[] args) {

        //testListIsEmpty();
        //testRegex();
    }

    private static void testListIsEmpty() {
        List<String> list = new ArrayList<>();
        System.out.println("no element: "+list.isEmpty()+" size: "+list.size()+list);
        list.add(null);
        list.add(null);
        System.out.println("add null twice: "+list.isEmpty()+" size: "+list.size()+list);
    }

    public static void testRegex(){
        String t = "Iam\n\n good\rhow\r\nabout you? #tag au help #test, #new    good";
        System.out.println(t);
        System.out.println(t.replaceAll("[^-a-zA-Z0-9 ]"," ").replaceAll("  +"," "));
    }

    public static void testFileWriter() throws IOException{
        String dir = "E://data/why_are_you_so_stupid.txt";
        String[] in = {"you guess","Guess again","Final guess"};
        Gson gson = new GsonBuilder().create();

        TumblrPost p = new TumblrPost(122L, "type","text","date","blog_name",
                Arrays.asList("a","b"));
        TumblrPost p1 = new TumblrPost(122L, "type","text","date","blog_name",
                Arrays.asList());
        FileWriter fw = new FileWriter(dir);
        String jo = gson.toJson(p);
        String jo1 = gson.toJson(p1);
        fw.writeSingleLine(jo);
        fw.writeSingleLine(jo1);
        System.out.println(jo1);

        /*for(String i:in){
            fw.writeSingleLine(i);
        }*/
        fw.close();
    }
}
