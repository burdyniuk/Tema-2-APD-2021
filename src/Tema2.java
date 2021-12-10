import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Tema2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        // initialize variables
        String in_file = args[1], out_file = args[2];
        int n_workers = Integer.parseInt(args[0]);
        int n_docs = 0;
        int D = 0;
        String[] files = new String[100];

        // read from test file
        try {
            File myObj = new File(in_file);
            Scanner myReader = new Scanner(myObj);
            D = myReader.nextInt();
            n_docs = myReader.nextInt();
            files = new String[n_docs];
            myReader.nextLine();
            for (int i = 0; i < n_docs; i++) {
                files[i] = myReader.nextLine();
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        // initialize variables needed for tasks
        ExecutorService mapExecutors = Executors.newFixedThreadPool(n_workers);
        ExecutorService reduceExecutors = Executors.newFixedThreadPool(n_workers);
        AtomicInteger mapInt = new AtomicInteger(n_docs);
        AtomicInteger reduceInt = new AtomicInteger(n_docs);
        MapTask[] mapTasks = new MapTask[n_docs];
        ReduceTask[] reduceTasks = new ReduceTask[n_docs];
        ArrayList<ArrayList<HashMap<Integer, Integer>>> mapRes = new ArrayList<>();
        ArrayList<ArrayList<ArrayList<String>>> mapWords = new ArrayList<>();

        // create map tasks and run them
        for (int i = 0; i < n_docs; i++) {
            mapRes.add(i, new ArrayList<>());
            mapWords.add(i, new ArrayList<>());
            mapTasks[i] = new MapTask(files[i], 0, D, mapExecutors, mapRes.get(i), mapWords.get(i), mapInt);
            mapExecutors.submit(mapTasks[i]);
        }

        // wait for termination of all map tasks
        try {
            if (!mapExecutors.awaitTermination(1000, TimeUnit.SECONDS)) {
                mapExecutors.shutdownNow();
            }
        } catch (InterruptedException ex) {
            mapExecutors.shutdownNow();
        }

        // create reduce tasks and run them
        for (int i = 0; i < n_docs; i++) {
            reduceTasks[i] = new ReduceTask(files[i], mapRes.get(i), mapWords.get(i), reduceInt, reduceExecutors);
            reduceExecutors.submit(reduceTasks[i]);
        }

        // wait for termination of reduce tasks
        try {
            if (!reduceExecutors.awaitTermination(1000, TimeUnit.SECONDS)) {
                reduceExecutors.shutdownNow();
            }
        } catch (InterruptedException ex) {
            reduceExecutors.shutdownNow();
        }

        // sort results by rang in descending order
        Arrays.sort(reduceTasks, new Sorting());

        FileWriter fileWriter = new FileWriter(out_file);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        // write final results to output file
        for (int i = 0; i < n_docs; i++) {
            String rg = String.format("%.2f", reduceTasks[i].getRang());
            printWriter.print(reduceTasks[i].getDocName() + "," + rg + "," + reduceTasks[i].getMaxLen()
                    + "," + reduceTasks[i].getNrAppearance() + "\n");
        }

        printWriter.close();
    }
}

/**
 * Class for sorting reduce tasks by computed rangs.
 */
class Sorting implements Comparator<ReduceTask> {
    public int compare(ReduceTask a, ReduceTask b)
    {
        return Float.compare(b.getRang(), a.getRang());
    }
}
