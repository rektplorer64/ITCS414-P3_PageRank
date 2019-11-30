/*
This Code is modified by Section 1 Students of Mahidol University, the Faculty of ICT, 2019
as part of the third project of ITCS414 - Information Retrieval and Storage.

Project 3: PageRank

JAVA SDK Version 12+ is required to compile this source code.

The group consists of
    1. Krittin      Chatrinan       ID 6088022
    2. Anon         Kangpanich      ID 6088053
    3. Tanawin      Wichit          ID 6088221
 */

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

    /**
     * A boolean specifies Verbosity of the program
     */
    private boolean DEBUG_VERBOSE = false;

    /**
     * A boolean that indicates whether the program will print PageRank to a file or not
     */
    private boolean DEBUG_PAGE_RANK_OUTPUT_FILE = false;

    /**
     * The constant specifies Damping Factor for PageRank
     */
    private static final double DAMPING_FACTOR = 0.85;

    /**
     * Mapping between Page Id (namely A) and a Set of Page Ids that link to A.
     * TODO: This data structure can be omitted and embed into Page Class.
     *
     */
    private HashMap<Integer, HashSet<Integer>> graphDataStore = new HashMap<>();

    /**
     * Mapping between Page Id and its associate Page Object
     */
    private HashMap<Integer, Page> pageMap = new HashMap<>();

    /**
     * The Limit constant for convergence count
     */
    private final int CONVERGENCE_ITER_COUNT_LIMIT = 4;

    /**
     * The initial convergence count
     */
    private int convergenceIterCount = 1;

    /**
     * Temporary Perplexity for determining convergences
     */
    private double lastPerplexity = 0;

    /**
     * This class reads the direct graph stored in the file "inputLinkFilename" into memory.
     * Each line in the input file should have the following format:
     * [pid_1] [pid_2] [pid_3] .. [pid_n]
     * <p>
     * Where pid_1, pid_2, ..., pid_n are the page IDs of the page having links to page pid_1.
     * You can assume that a page ID is an integer.
     *
     * @param inputLinkFilename input file that contains page Id and its linked
     */
    public void loadData(String inputLinkFilename) {

        // Data stream for String
        Stream<String> dataStream;
        try {
            // Read from the file line-by-line using UTF-8 Character set
            dataStream = Files.lines(Paths.get(inputLinkFilename), StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // For each line, splits the page Id
        dataStream.forEach(line -> {

            // Split page Ids by white spaces
            String[] parts = line.split("\\s");

            // Parses the first part to an Integer -> This is the mainPageId.
            int mainPageId = Integer.parseInt(parts[0]);

            // If the page map does not contain the Page Id
            if (!pageMap.containsKey(mainPageId)) {
                // Put the Page Id into the map
                pageMap.put(mainPageId, new Page(mainPageId));
            }

            // Initializes empty set for Page Id that associates to the mainPageId
            HashSet<Integer> connectedNode = new HashSet<>();

            // Iterates thru all Page Id that associates to the mainPageId
            for (int i = 1; i < parts.length; i++) {
                // Parse each Page Id into an Integer
                final int pageId = Integer.parseInt(parts[i]);

                // Add the parsed integer into the set
                connectedNode.add(pageId);

                // If the page map does not contain the Page Id
                if (!pageMap.containsKey(pageId)) {
                    // Put the Page Id into the map
                    pageMap.put(pageId, new Page(pageId));
                }

                // Counts up the Out-Link of the Page Id by 1
                // Out-Link means that if A links to P, Q, R, S, then A has 4 out-links.
                pageMap.get(pageId).incrementOutLinkCount();
            }

            // Put the Set into the Graph Map
            graphDataStore.put(mainPageId, connectedNode);
        });

        // System.out.println(graphDataStore);
    }

    /**
     * This method will be called after the graph is loaded into the memory.
     * <p>
     * This method initialize the parameters for the PageRank algorithm including
     * setting an initial weight to each page.
     */
    public void initialize() {

        // For every Page
        for (Map.Entry<Integer, Page> pageWeight : pageMap.entrySet()) {
            // Set an initial value
            pageWeight.getValue().setPageRank(1 / (double) pageMap.size());
        }

        if (DEBUG_VERBOSE) {
            System.out.println("TOTAL PAGES -> " + pageMap.size());
        }
    }

    /**
     * Computes the perplexity of the current state of the graph. The definition of perplexity is given in the project specs.
     *
     * @return perplexity value of the current
     */
    public double getPerplexity() {
        double entropy = 0;

        if (DEBUG_VERBOSE) {
            System.out.println("getPerplexity()");
        }

        // For each page inside the pageMap
        for (Map.Entry<Integer, Page> pageWeight : pageMap.entrySet()) {
            // Get the Page
            Page p = pageWeight.getValue();

            if (DEBUG_VERBOSE) {
                System.out.println("\tPage " + p.getId() + " PR = " + p.getPageRank());
            }

            // Calculate the Entropy and accumulate it
            entropy += p.getPageRank() * Math.log(p.getPageRank()) / Math.log(2);
        }

        if (DEBUG_VERBOSE) {
            System.out.println("Entropy = " + entropy);
        }
        return Math.pow(2, entropy * -1);
    }

    /**
     * Returns true if the perplexity converges (hence, terminate the PageRank algorithm).
     * Otherwise, Returns false otherwise (and PageRank algorithm continue to update the page scores).
     *
     * @return whether the current state is converged or not
     */
    public boolean isConverge() {

        // Calculate a new Perplexity
        double currentPerplexity = getPerplexity();

        if (DEBUG_VERBOSE) {
            System.out.println("isConverge() -> " + currentPerplexity + "\t\t\t" + lastPerplexity + " -> " + Math.floor(currentPerplexity) % 10 + ",\t" + Math.floor(lastPerplexity) % 10);
        }

        // If the unit digit of old and new Perplexity is the same
        if (Math.floor(currentPerplexity) % 10 == Math.floor(lastPerplexity) % 10) {
            // Counts up the convergence iteration count by 1
            convergenceIterCount++;
        } else {
            // Reset the count back to 1
            convergenceIterCount = 1;
        }

        // Make the new Perplexity an old one
        lastPerplexity = currentPerplexity;

        // If the count equals to the LIMIT
        if (convergenceIterCount == CONVERGENCE_ITER_COUNT_LIMIT) {
            // Reset the count back to 1
            convergenceIterCount = 1;

            // The PageRank is converge now
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
     *
     * @param perplexityOutFilename the url of the output file that contains perplexity value from each iteration.
     * @param prOutFilename         the url of the output file that contains the final PageRank score
     */
    public void runPageRank(String perplexityOutFilename, String prOutFilename) {

        int totalPage = pageMap.size();

        List<String> perplexities = new ArrayList<>();
        List<String> pageRankScores = new ArrayList<>();
        while (!isConverge()) {

            // Calculate total sink P
            double sinkPageRank = 0;

            HashMap<Integer, Double> newPageRanks = new HashMap<>();

            for (int pageId : pageMap.keySet()) {
                if (pageMap.get(pageId).isSinkPage()) {
                    sinkPageRank += pageMap.get(pageId).getPageRank();
                }
            }


            // For each Page in the pageMap
            for (int pageId : pageMap.keySet()) {
                // Teleportation
                double newPageRank = (1 - DAMPING_FACTOR) / (double) totalPage;

                // Spread remaining sink PR evenly
                newPageRank += DAMPING_FACTOR * sinkPageRank / (double) totalPage;

                if (DEBUG_VERBOSE) {
                    System.out.println("\nFinding the ones who link to " + pageId);
                }

                // If the graph does contain the pageId AND the Page being linked by other page(s).
                if (graphDataStore.get(pageId) != null && !graphDataStore.get(pageId).isEmpty()) {
                    // For each pages pointing to the page w/ id = pageId
                    for (int linkedPage : graphDataStore.get(pageId)) {
                        final Page incomingPage = pageMap.get(linkedPage);

                        // Add share of PageRank from in-links
                        newPageRank += DAMPING_FACTOR * incomingPage.getPageRank() / (double) incomingPage.getOutLinkCount();

                        if (DEBUG_VERBOSE) {
                            System.out.println("\tPage " + incomingPage.getId() + " has " + incomingPage.getOutLinkCount() + " pages it links to.");
                        }
                    }
                }

                if (DEBUG_VERBOSE) {
                    System.out.println("NewPageRank for " + pageId + " -> " + newPageRank + "\n");
                }

                // Collect the Page Rank associated with a Page Id for the final result
                newPageRanks.put(pageId, newPageRank);
            }

            // For all pages, set a new rank.
            for (Map.Entry<Integer, Page> pageEntry : pageMap.entrySet()) {
                pageEntry.getValue().setPageRank(newPageRanks.get(pageEntry.getKey()));
            }

            // Add a Perplexity to the Perplexities Map as a String
            perplexities.add(String.valueOf(getPerplexity()));
        }

        // For all pages, add all PageRanks to the PageRank list.
        for (Map.Entry<Integer, Page> pageEntry : pageMap.entrySet()) {
            pageRankScores.add(pageEntry.getKey() + " " + pageEntry.getValue().getPageRank());
        }

        // Write Perplexities into the output file
        writeStringToFile(perplexityOutFilename, perplexities);

        // Write PageRanks into the output file
        writeStringToFile(prOutFilename, pageRankScores);
    }

    /**
     * Write a List of Strings (1 Line per String) into a text file
     *
     * @param fileIdentifier Url of the file
     * @param lines List of Strings to be written
     */
    private void writeStringToFile(String fileIdentifier, List<String> lines) {

        // Initialize the Path based on the Url
        Path file = Paths.get(fileIdentifier);

        // Initialize the File based on the Url
        File f = new File(fileIdentifier);

        // If the file exists
        if (f.exists()) {
            // Delete the file
            f.delete();
        }

        try {
            // Write the Given array into the file
            Files.write(file, lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Return the top K page IDs, whose scores are highest.
     *
     * @param K the maximum value of the ranked page Id results
     * @return the ranked page Id results
     */
    public Integer[] getRankedPages(int K) {

        // Copy the Pages from the pageMap
        ArrayList<Page> pageArrayList = new ArrayList<>(pageMap.values());

        // Sort the Pages by PageRank
        pageArrayList.sort(Page::compareTo);

        if (DEBUG_PAGE_RANK_OUTPUT_FILE){
            // Print PageId along with PageRank Score into a file for DEBUGGING
            ArrayList<String> lines = new ArrayList<>();
            for (Page p : pageArrayList){
                lines.add(p.getId() + "\t" + p.getPageRank());
            }
            writeStringToFile("RawSortedPageRank.txt", lines);
        }

        // Determine the proper size for the result Array
        int arraySize = Math.min(K, pageArrayList.size());

        // Initialize result array with the size equals to the previously determined num
        Integer[] topKResults = new Integer[arraySize];

        // Slice the array from the first to the element with index equal to <the size - 1>
        List<Page> sortedSlicedResults = pageArrayList.subList(0, arraySize);

        // Add all results into the result Integer array
        for (int i = 0; i < arraySize; i++) {
            topKResults[i] = sortedSlicedResults.get(i).getId();
        }

        return topKResults;
    }

    public static void main(String args[]) {
        long startTime = System.currentTimeMillis();

        PageRanker pageRanker = new PageRanker();
        pageRanker.loadData("citeseer.dat");
        // pageRanker.loadData("p3_testcase/test.dat");
        pageRanker.initialize();
        pageRanker.runPageRank("perplexity.out", "pr_scores.out");

        Integer[] rankedPages = pageRanker.getRankedPages(100);

        double estimatedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;

        System.out.println("Top 100 Pages are:\n" + Arrays.toString(rankedPages));
        System.out.println("Processing time: " + estimatedTime + " seconds");
    }

    /**
     * A class that represents a Page
     */
    private static class Page implements Comparable {

        /**
         * Integer that represents Page Id
         */
        private int id;

        /**
         * Double Precision value that represents PageRank value
         */
        private double pageRank;

        /**
         * Integer that indicates how many links that THIS PAGE points to.
         */
        private int outLinkCount = 0;

        /**
         * Default constructor for Page class
         * @param id page Id
         */
        Page(int id) {
            this(id, 0.0);
        }

        /**
         * Constructor for Page class
         * @param id Page Id
         * @param pageRank Page rank value
         */
        Page(int id, double pageRank) {
            this.id = id;
            this.pageRank = pageRank;
        }

        double getPageRank() {
            return pageRank;
        }

        void setPageRank(double pageRank) {
            this.pageRank = pageRank;
        }

        boolean isSinkPage() {
            return outLinkCount == 0;
        }

        int getId() {
            return id;
        }

        int getOutLinkCount() {
            return outLinkCount;
        }

        /**
         * Increment Out-Link Count by 1
         */
        void incrementOutLinkCount() {
            outLinkCount++;
        }

        @Override
        public int compareTo(Object o) {
            // If the object is not an instance of Page class
            if (!(o instanceof Page)) {
                // It is not comparable.
                return -1;
            }

            // Compare PageRank score in Descending Order manner
            int rankComparison = Double.compare(((Page) o).pageRank, this.pageRank);

            // If both score is equal, compare them by Id instead
            if (rankComparison == 0){
                return this.id - ((Page) o).id;
            }
            return rankComparison;
        }
    }
}
