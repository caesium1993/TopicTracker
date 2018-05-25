package dedup;

import clojure.lang.IFn;
import org.apache.commons.lang.StringUtils;
import utils.FileWriter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class MergeIdSet {

    public static void main(String[] args) throws IOException {
        FileReader fr = new FileReader("E://data/storm/tem_id.txt");
        BufferedReader br = new BufferedReader(fr);
        HashSet<Long> idSet = new HashSet<>();
        FileWriter fileWriter = new FileWriter("E://data/storm/post_id.txt",false);

        String str = null;
        while (StringUtils.isNotBlank(str = br.readLine())){
            idSet.add(Long.valueOf(str));
            fileWriter.writeSingleLine(str);
        }

        br.close();

        BufferedReader br2 = new BufferedReader(new FileReader("E://data/post_id.txt"));
        String line = null;
        while(StringUtils.isNotBlank(line = br2.readLine())){
            if(idSet.add(Long.valueOf(line))){
                fileWriter.writeSingleLine(line);
            }
        }
        fileWriter.close();
        br2.close();

        System.out.println("id set size: "+idSet.size());

    }
}
