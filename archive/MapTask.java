import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class MapTask implements Runnable {
    private final String docName;
    private final int offset;
    private final int size;
    private final ExecutorService tpe;
    private final ArrayList<HashMap<Integer, Integer>> results;
    private final ArrayList<ArrayList<String>> maxWords;
    private final AtomicInteger nrTasks;

    public MapTask(String docName, int offset, int size, ExecutorService tpe, ArrayList<HashMap<Integer,
            Integer>> results, ArrayList<ArrayList<String>> maxWords, AtomicInteger nrTasks) {
        this.docName = docName;
        this.offset = offset;
        this.size = size;
        this.tpe = tpe;
        this.results = results;
        this.maxWords = maxWords;
        this.nrTasks = nrTasks;
    }

    @Override
    public void run() {
        // read from file the size characters from offset
        try {
            HashMap<Integer, Integer> partialRes = new HashMap<Integer, Integer>();
            ArrayList<String> mWords = new ArrayList<String>();
            int o = 0;
            int maxLength = 0;
            // keep only words
            String separators = "\\W+";
            RandomAccessFile input = new RandomAccessFile(docName, "r");
            byte[] chars = new byte[size];
            input.seek(offset);
            int r = input.read(chars);
            input.close();
            // verify if the last task to shutdown executor service
            if (r == -1) {
                int tasks = nrTasks.decrementAndGet();
                if (tasks == 0) {
                    tpe.shutdown();
                }
                return;
            }

            // if read characters are fewer than size, copy only the needed characters from bytes array
            String s;
            if (r < size) {
                byte[] newChars = new byte[r];
                System.arraycopy(chars, 0, newChars, 0, newChars.length);
                s = new String(newChars, StandardCharsets.UTF_8);
            } else {
                s = new String(chars, StandardCharsets.UTF_8);
            }

            // verify if string is ending in the middle of the word
            if (Character.isLetter(s.charAt(r-1)) && r == size) {
                input = new RandomAccessFile(docName, "r");
                chars = new byte[size];
                input.seek(offset + size);
                r = input.read(chars);
                input.close();
                if (r > 0) {
                    String nextS = new String(chars, StandardCharsets.UTF_8);
                    // keep only the termination of last word
                    if (Character.isLetter(nextS.charAt(0))) {
                        String[] nextWords = nextS.split(separators);
                        s += nextWords[0];
                        o = nextWords[0].length();
                    }
                }
            }

            // split in words array
            String[] words = s.split(separators);
            for (String word : words) {
                if (word.length() == 0)
                    continue;
                // add to partial dictionary
                if (partialRes.containsKey(word.length())) {
                    int k = partialRes.get(word.length());
                    k++;
                    partialRes.put(word.length(), k);

                } else {
                    partialRes.put(word.length(), 1);
                }
                if (word.length() == maxLength) {
                    mWords.add(word);
                }
                if (word.length() > maxLength) {
                    mWords = new ArrayList<String>();
                    maxLength = word.length();
                    mWords.add(word);
                }
            }
            // add the results of this task to results
            results.add(partialRes);
            maxWords.add(mWords);

            // run next task on incremented offset
            tpe.submit(new MapTask(docName, offset + size + o, size, tpe, results, maxWords, nrTasks));
        } catch (IOException e) {
            e.printStackTrace();
            tpe.shutdown();
        }
    }
}
