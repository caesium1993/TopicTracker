package utils;

import type.TumblrPost;

import java.io.*;
import java.util.*;

public class FileWriter {

    private File file;
    private FileOutputStream fileOutputStream;
    private BufferedWriter bw;

    public FileWriter(String dir) throws FileNotFoundException {
        this.file = new File(dir);
        this.fileOutputStream = new FileOutputStream(this.file,true);
        this.bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
    }

    public FileWriter(String dir, Boolean append) throws FileNotFoundException {
        this.file = new File(dir);
        this.fileOutputStream = new FileOutputStream(this.file,append);
        this.bw = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
    }

    public void write(String dir, HashSet<Long> posts) throws IOException {
        File file = new File(dir);
        FileOutputStream out = new FileOutputStream(file,true);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));

        Iterator<Long> iterator = posts.iterator();
        while ( iterator.hasNext()) {
            long p = iterator.next();
            bw.write(String.valueOf(p) + "\n");
            bw.flush();
        }
    }

    public void writeSingleLine(String line) throws IOException {
        this.bw.write(line);
        this.bw.newLine();
        this.bw.flush();
    }

    public void close() throws IOException {
        this.bw.close();
    }
}