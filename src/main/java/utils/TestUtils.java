package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import type.TumblrPost;
import utils.FileWriter;

import java.io.*;
import java.util.*;

public class TestUtils {
    public static void main(String[] args) {
        //testListIsEmpty();
        //testRegex();



    }

    private void testIsOnTopic(){
        String test = "1 a b 3 g h zz zzzzz zzz oo yyy nn cj f";
        System.out.println(isOnTopic(test, ""));
    }

    private static boolean isOnTopic(String t, String p) {
        int flagMatch = 0;
        HashMap<String, List<String>> tem = new HashMap<>();

        tem.put("1", Arrays.asList("a","b"));
        tem.put("2", Arrays.asList("c","d"));
        tem.put("3", Arrays.asList("e","f"));
        tem.put("4", Arrays.asList("g","h"));
        tem.put("5", Arrays.asList("i","j"));

        List<String> matches = Arrays.asList("1");
        System.out.println(matches);

        if(matches.size()>=3){
            return true;
        } else if (matches.size()==0){
            return false;
        }

        for(String m:matches){
            if(tem.containsKey(m.trim())){
                tem.remove(m, tem.get(m));
            }
        }

        Iterator iterator = tem.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry) iterator.next();
            List<String> seeds = (List<String>) entry.getValue();

            for(String s:seeds){
                if(t.contains(s.trim())||t.contains(s.replaceAll("_"," ").trim())){
                    flagMatch++;
                    break;
                }
            }

        }

        int sum = flagMatch+matches.size();
        System.out.println("finally sum is: "+sum);
        if(sum>=3){
            return true;
        } else{
            return false;
        }
    }

    private void testAddIntoArray(){
        String[] test = {"aa","bb"};
        List<String> newList = new ArrayList<>();
        newList.add("cc");
        newList.add("dd");
        newList.add("ee");

        int length = test.length+newList.size();
        String[] tem = new String[length];
        for(int i=0;i<test.length;i++){
            tem[i] = test[i];
        }
        for(int j=0;j<newList.size();j++){
            tem[j+test.length] = newList.get(j);
        }

        test = tem;

        System.out.println(Arrays.asList(test));
    }

    private static void readTest() throws IOException, InterruptedException {
        File file = new File("E://data/readTest.txt");
        BufferedReader br = new BufferedReader(new FileReader(file));

        String str = null;
        while (true){
            if((str=br.readLine())!=null){
                System.out.println(str);
            } else {
                Thread.sleep(1000L);
            }

        }
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
