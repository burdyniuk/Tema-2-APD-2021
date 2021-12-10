import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class ReduceTask implements Runnable {
    private final String docName;
    private final ArrayList<HashMap<Integer, Integer>> results;
    private final ArrayList<ArrayList<String>> maxWords;
    private final AtomicInteger nrTasks;
    private float rang;
    private int maxLen;
    private int nrAppearance;
    private final ExecutorService tpe;

    public ReduceTask(String docName, ArrayList<HashMap<Integer, Integer>> results,
                      ArrayList<ArrayList<String>> maxWords, AtomicInteger nrTasks, ExecutorService tpe) {
        this.docName = docName;
        this.results = results;
        this.maxWords = maxWords;
        this.nrTasks = nrTasks;
        this.tpe = tpe;
    }

    public String getDocName() {
        String[] name = docName.split("/");
        return name[name.length - 1];
    }

    public float getRang() {
        return rang;
    }

    public int getMaxLen() {
        return maxLen;
    }

    public int getNrAppearance() {
        return nrAppearance;
    }

    // function to compute the fibonacci by the length of word
    private int fib(int position) {
        int first = 0;
        int second = 1;
        int aux = 0;
        if (position == 1)
            return 1;

        for (int i = 2; i < position+2; i++) {
            aux = first + second;
            first = second;
            second = aux;
        }

        return aux;
    }

    @Override
    public void run() {
        // unify the partial result in one dictionary
        HashMap<Integer, Integer> finalRes = new HashMap<>();
        for (HashMap res : results) {
            Set entrySet = res.entrySet();
            Iterator it = entrySet.iterator();
            while (it.hasNext()) {
                HashMap.Entry entry = (HashMap.Entry)it.next();
                // if there is already this key, increment it by the count of words
                if (finalRes.containsKey(entry.getKey())) {
                    Integer value = (Integer) entry.getValue();
                    value += (int) finalRes.get(entry.getKey());
                    finalRes.put((Integer) entry.getKey(), value);
                } else {
                    // add first entry with this key
                    finalRes.put((Integer) entry.getKey(), (Integer) entry.getValue());
                }
            }
        }

        Set entrySet = finalRes.entrySet();
        Iterator it = entrySet.iterator();
        float sum = 0;
        int nr_words = 0;
        int maxLength = 0;
        int app = 0;
        while (it.hasNext()) {
            HashMap.Entry entry = (HashMap.Entry) it.next();
            // compute "fibonacci" on this word length
            int f = fib((int) entry.getKey());
            // verify if this is the biggest word to save it length and number of appearance in document
            if ((int) entry.getKey() > maxLength) {
                maxLength = (int) entry.getKey();
                app = (int) entry.getValue();
            }
            // multiply by number of appearance and sum it
            sum += f * (int) entry.getValue();
            nr_words += (int) entry.getValue();
        }

        // compute the rang of document
        rang = sum / nr_words;
        maxLen = maxLength;
        nrAppearance = app;

        // decrement number of tasks
        int tasks = nrTasks.decrementAndGet();

        // if this is the last one, shutdown executor service
        if (tasks == 0) {
            tpe.shutdown();
        }
    }
}
