//Name(s):
//ID
//Section

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * This class implements PageRank algorithm on simple graph structure.
 * Put your name(s), ID(s), and section here.
 */
public class PageRanker {

    private static final double DAMPING_FACTOR = 0.85;

    private HashMap<Integer, HashSet<Integer>> graphDataStore = new HashMap<>();

    private HashMap<Integer, Page> allPages = new HashMap<>();
    private boolean debugVerbose = false;

    /**
     * This class reads the direct graph stored in the file "inputLinkFilename" into memory.
     * Each line in the input file should have the following format:
     * [pid_1] [pid_2] [pid_3] .. [pid_n]
     * <p>
     * Where pid_1, pid_2, ..., pid_n are the page IDs of the page having links to page pid_1.
     * You can assume that a page ID is an integer.
     */
    public void loadData(String inputLinkFilename) {
        Stream<String> dataStream;
        try {
            dataStream = Files.lines(Paths.get(inputLinkFilename), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        dataStream.forEach(line -> {
            String[] parts = line.split("\\s");

            int key = Integer.parseInt(parts[0]);

            if (!allPages.containsKey(key)) {
                allPages.put(key, new Page(key));
            }

            HashSet<Integer> connectedNode = new HashSet<>();
            for (int i = 1; i < parts.length; i++) {
                final int pageId = Integer.parseInt(parts[i]);
                connectedNode.add(pageId);
                if (!allPages.containsKey(pageId)) {
                    allPages.put(pageId, new Page(pageId));
                }
                allPages.get(pageId).incrementOutLinkCount();
            }

            graphDataStore.put(key, connectedNode);
        });

        // System.out.println(graphDataStore);
    }

    /**
     * This method will be called after the graph is loaded into the memory.
     * This method initialize the parameters for the PageRank algorithm including
     * setting an initial weight to each page.
     */
    public void initialize() {

        for (Map.Entry<Integer, Page> pageWeight : allPages.entrySet()) {
            pageWeight.getValue().setPageRank(1 / (double) allPages.size());
        }

        if (debugVerbose) {
            System.out.println("TOTAL PAGES -> " + allPages.size());
        }
    }

    /**
     * Computes the perplexity of the current state of the graph. The definition
     * of perplexity is given in the project specs.
     */
    public double getPerplexity() {
        double entropy = 0;

        if (debugVerbose) {
            System.out.println("getPerplexity()");
        }

        for (Map.Entry<Integer, Page> pageWeight : allPages.entrySet()) {
            Page p = pageWeight.getValue();
            if (debugVerbose) {
                System.out.println("\tPage " + p.getId() + " PR = " + p.getPageRank());
            }
            entropy += p.getPageRank() * Math.log(p.getPageRank()) / Math.log(2);
        }

        if (debugVerbose) {
            System.out.println("Entropy = " + entropy);
        }
        return Math.pow(2, entropy * -1);
    }

    private final int CONVERGENCE_ITER_COUNT_LIMIT = 4;
    private int convergenceIterCount = 1;
    private double lastPerplexity = 0;

    /**
     * Returns true if the perplexity converges (hence, terminate the PageRank algorithm).
     * Returns false otherwise (and PageRank algorithm continue to update the page scores).
     */
    public boolean isConverge() {
        double currentPerplexity = getPerplexity();

        if (debugVerbose) {
            System.out.println("isConverge() -> " + currentPerplexity + "\t\t\t" + lastPerplexity + " -> " + Math.floor(currentPerplexity) % 10 + ",\t" + Math.floor(lastPerplexity) % 10);
        }

        if (Math.floor(currentPerplexity) % 10 == Math.floor(lastPerplexity) % 10) {
            convergenceIterCount++;
        } else {
            convergenceIterCount = 1;
        }

        lastPerplexity = currentPerplexity;

        if (convergenceIterCount == CONVERGENCE_ITER_COUNT_LIMIT) {
            convergenceIterCount = 1;
            return true;
        }

        return false;
    }


    /**
     * The main method of PageRank algorithm.
     * Can assume that initialize() has been called before this method is invoked.
     * While the algorithm is being run, this method should keep track of the perplexity
     * after each iteration.
     * <p>
     * Once the algorithm terminates, the method generates two output files.
     * [1]	"perplexityOutFilename" lists the perplexity after each iteration on each line.
     * The output should look something like:
     * <p>
     * 183811
     * 79669.9
     * 86267.7
     * 72260.4
     * 75132.4
     * <p>
     * Where, for example,the 183811 is the perplexity after the first iteration.
     * <p>
     * [2] "prOutFilename" prints out the score for each page after the algorithm terminate.
     * The output should look something like:
     * <p>
     * 1	0.1235
     * 2	0.3542
     * 3 	0.236
     * <p>
     * Where, for example, 0.1235 is the PageRank score of page 1.
     */
    public void runPageRank(String perplexityOutFilename, String prOutFilename) {

        int totalPage = allPages.size();

        List<String> perplexities = new ArrayList<>();
        List<String> pageRankScores = new ArrayList<>();
        while (!isConverge()) {
            double sinkPageRank = 0;

            HashMap<Integer, Double> newPageRanks = new HashMap<>();

            for (int pageId : allPages.keySet()) {
                if (allPages.get(pageId).isSinkPage()) {
                    sinkPageRank += allPages.get(pageId).getPageRank();
                }
            }

            for (int pageId : allPages.keySet()) {
                double newPageRank = (1 - DAMPING_FACTOR) / (double) totalPage;
                newPageRank += DAMPING_FACTOR * sinkPageRank / (double) totalPage;

                if (debugVerbose) {
                    System.out.println("\nFinding the ones who link to " + pageId);
                }
                if (graphDataStore.get(pageId) != null && !graphDataStore.get(pageId).isEmpty()) {
                    for (int linkedPage : graphDataStore.get(pageId)) {
                        final Page incomingPage = allPages.get(linkedPage);

                        newPageRank += DAMPING_FACTOR * incomingPage.getPageRank() / (double) incomingPage.getOutLinkCount();

                        if (debugVerbose) {
                            System.out.println("\tPage " + incomingPage.getId() + " has " + incomingPage.getOutLinkCount() + " pages it links to.");
                        }
                    }
                }

                if (debugVerbose) {
                    System.out.println("NewPageRank for " + pageId + " -> " + newPageRank + "\n");
                }

                newPageRanks.put(pageId, newPageRank);
            }

            // System.out.println("Iter: " + newPageRanks);
            for (Map.Entry<Integer, Page> pageEntry : allPages.entrySet()) {
                pageEntry.getValue().setPageRank(newPageRanks.get(pageEntry.getKey()));
            }

            perplexities.add(String.valueOf(getPerplexity()));
        }

        for (Map.Entry<Integer, Page> pageEntry : allPages.entrySet()) {
            pageRankScores.add(pageEntry.getKey() + " " + pageEntry.getValue().getPageRank());
        }

        writeStringToFile(perplexityOutFilename, perplexities);
        writeStringToFile(prOutFilename, pageRankScores);
    }


    private void writeStringToFile(String fileIdentifier, List<String> lines) {
        Path file = Paths.get(fileIdentifier);
        File f = new File(fileIdentifier);

        if (f.exists()) {
            f.delete();
        }
        try {
            Files.write(file, lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Return the top K page IDs, whose scores are highest.
     */
    public Integer[] getRankedPages(int K) {
        ArrayList<Page> pageArrayList = new ArrayList<>(allPages.values());
        pageArrayList.sort(Page::compareTo);

        int arraySize = Math.min(K, pageArrayList.size());

        Integer[] topKResults = new Integer[arraySize];

        List<Page> sortedSlicedResults = pageArrayList.subList(0, arraySize);

        for (int i = 0; i < arraySize; i++) {
            topKResults[i] = sortedSlicedResults.get(i).getId();
        }

        return topKResults;
    }

    public static void main(String args[]) {
        long startTime = System.currentTimeMillis();

        PageRanker pageRanker = new PageRanker();
        // pageRanker.loadData("citeseer.dat");
        pageRanker.loadData("p3_testcase/test.dat");
        pageRanker.initialize();
        pageRanker.runPageRank("perplexity.out", "pr_scores.out");

        Integer[] rankedPages = pageRanker.getRankedPages(100);

        double estimatedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;

        System.out.println("Top 100 Pages are:\n" + Arrays.toString(rankedPages));
        System.out.println("Processing time: " + estimatedTime + " seconds");
    }

    public class Page implements Comparable {

        private int id;
        private double pageRank;
        private int outLinkCount = 0;

        public Page(int id) {
            this(id, 0.0);
        }

        public Page(int id, double pageRank) {
            this.id = id;
            this.pageRank = pageRank;
        }

        public double getPageRank() {
            return pageRank;
        }

        public void setPageRank(double pageRank) {
            this.pageRank = pageRank;
        }

        public boolean isSinkPage() {
            return outLinkCount == 0;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getOutLinkCount() {
            return outLinkCount;
        }

        public void incrementOutLinkCount() {
            outLinkCount++;
        }

        public void setOutLinkCount(int outLinkCount) {
            this.outLinkCount = outLinkCount;
        }

        @Override
        public int compareTo(Object o) {
            if (!(o instanceof Page)){
                return -1;
            }
            return Double.compare(((Page) o).pageRank, this.pageRank);
        }
    }
}
