package estimate;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.text.DecimalFormat;

public class ConfusionMatrix {
    private static String dirPrefix = "E://data/storm/exp_terrorism_middle_east/table_";
    private static String dirOffTable = "off_topic_round_";
    private static String dirOnTable = "round_";
    private static String dirPostfix = ".json";
    private static int limit = 2;

    public static void main(String[] args) throws IOException {
        int ttp = 0;
        int tfp = 0;
        int tfn = 0;
        int ttn = 0;

        for(int i=0;i<=limit;i++){
            int tp = 0;
            int fp = 0;
            int fn = 0;
            int tn = 0;

            String dirOff = dirPrefix+dirOffTable+Integer.toString(i)+dirPostfix;
            String dirOn = dirPrefix+dirOnTable+Integer.toString(i)+dirPostfix;
            FileInputStream inOn = new FileInputStream(new File(dirOn));
            FileInputStream inOff = new FileInputStream(new File(dirOff));

            BufferedReader brOn = new BufferedReader(new InputStreamReader(inOn));
            BufferedReader brOff = new BufferedReader(new InputStreamReader(inOff));

            String line1 = brOn.readLine();
            while(StringUtils.isNotBlank(line1=brOn.readLine())){
                int j = Integer.parseInt(Character.toString(line1.charAt(0)));
                if(j==0){
                    fp++;
                }else if(j==1){
                    tp++;
                }
            }

            String line2 = brOff.readLine();
            while(StringUtils.isNotBlank(line2=brOff.readLine())){
                int z = Integer.parseInt(Character.toString(line2.charAt(0)));
                if(z==0){
                    tn++;
                }else if(z==1){
                    fn++;
                }
            }

            System.out.println("*************Round "+i+"***************");
            showConfusionMatrix(tp,fp,fn,tn);
            ttp+=tp;
            tfp+=fp;
            tfn+=fn;
            ttn+=tn;
        }

        System.out.println("**************total confusion matrix**************");
        showConfusionMatrix(ttp,tfp,tfn,ttn);

    }

    private static void showConfusionMatrix(int tp, int fp, int fn, int tn) {
        System.out.println("tp="+tp+"   "+"fn="+fn);
        System.out.println("fp="+fp+"   "+"tn="+tn);

        DecimalFormat df =new DecimalFormat("0.0000");
        float precision = (float)tp/(float)(tp+fp);
        System.out.printf("precision=%.4f",precision);
        System.out.println();
        float recall = (float)tp/(float)(tp+fn);
        System.out.printf("recall=%.4f",recall);
        System.out.println();
    }


}
