/* ---------------------------------------------------------------
Práctica 1.
Código fuente: Indexing.java
Grau Informàtica
Aleix Segura Paz.
Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Indexing {
    private static Indexing app;

    private static final int DEFAULT_VIRTUAL_THREAD_FACTOR = 150; // multiplies this value to number of files in order to create more Virtual Threads.
    private static final String DEFAULT_INDEX_DIR = "." + File.separator + "Index" + File.separator; ///

    private final String inputDirPath;
    private final String indexDirPath;
    private final int virtualThreadFactor;

    private final ConcurrentHashMap<Long, String> filesIdsMap = new ConcurrentHashMap<>(); // filesIds Data Structure
    private final ConcurrentHashMap<Location, String> fileLines = new ConcurrentHashMap<>(); // fileLines Data Structure
    private final ArrayList<ConcurrentHashMap<Location, String>> sameSizeFileLines = new ArrayList<>(); // Rescaled in order to force each virtual thread does the same work.

    private final ArrayList<ConcurrentHashMap<String, String>> unsortedInvertedIndex = new ArrayList<>();
    private ConcurrentHashMap<String, ArrayList<String>> invertedIndex = new ConcurrentHashMap<>(); // invertedIndex Data Structure

    private Thread fileIdSaver;
    private Thread fileFinder;
    private Thread[] linesContentVirtualThreads;
    private ReadFile[] readFilesTasks;
    private Thread[] invertedIndexBuilders;
    private InvertedIndexBuilder[] invertedIndexTasks;
    private Thread[] indexFilesBuilders;

    public Indexing(String inputDirPath) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = DEFAULT_INDEX_DIR;
        this.virtualThreadFactor = DEFAULT_VIRTUAL_THREAD_FACTOR;
    }

    public Indexing(String inputDirPath, String indexDirPath) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = indexDirPath;
        this.virtualThreadFactor = DEFAULT_VIRTUAL_THREAD_FACTOR;
    }

    public Indexing(String inputDirPath, String indexDirPath, int virtualThreadFactor) {
        this.inputDirPath = inputDirPath;
        this.indexDirPath = indexDirPath;
        this.virtualThreadFactor = virtualThreadFactor;
    }

    public static void setApp(Indexing app) { Indexing.app = app; }

    public static void main(String[] args) throws InterruptedException {
        if (args.length == 4) {
            app = new Indexing(args[1], args[2], Integer.parseInt(args[3]));

        } else if (args.length == 3) // args[0] is --enable-preview for Virtual Threads
            app = new Indexing(args[1], args[2]);
        else if (args.length == 2)
            app = new Indexing(args[1]);
        else
            printUsage();

        Instant start = Instant.now();

        app.searchTxtFiles(app.inputDirPath);
        app.joinThread(app.fileFinder);
        app.saveFileIds();

        app.buildFileLinesContent();
        app.joinThreads(app.linesContentVirtualThreads);
        app.addToGlobalLinesContent();

        app.constructFileLinesContent();
        app.constructSameSizeFileLines();

        app.constructInvertedIndex();
        app.joinThreads(app.invertedIndexBuilders);
        app.addToGlobalUnsortedInvertedIndex();
        app.invertedIndex = app.sortGlobalInvertedIndex();

        app.generateInvertedIndexFiles();
        app.joinThreads(app.indexFilesBuilders);
        app.joinThread(app.fileIdSaver);

        Instant end = Instant.now();
        System.out.println("Time to build the Inverted Index: " +
                Duration.between(start, end).toMillis() + " milliseconds.");
    }

    // Getters
    public String getAppInputDir() { return this.inputDirPath; }
    public String getAppIndexDir() { return this.indexDirPath; }
    public ConcurrentHashMap<Long, String> getFilesIdsMap() { return filesIdsMap; }
    public ConcurrentHashMap<Location, String> getFileLines() { return fileLines; }
    public ArrayList<ConcurrentHashMap<Location, String>> getSameSizeFileLines() { return sameSizeFileLines;}
    public ConcurrentHashMap<String, ArrayList<String>> getInvertedIndex() { return invertedIndex; }
    public Thread getFileFinder() { return fileFinder; }
    public Thread[] getIndexFilesBuilders() { return indexFilesBuilders; }
    public Thread[] getInvertedIndexBuilders() { return invertedIndexBuilders; }
    public Thread[] getLinesContentVirtualThreads() { return linesContentVirtualThreads; }

    /**
     * Generates threads that do the same amount of work and build the IndexFile_x.txt files.
     */
    public void generateInvertedIndexFiles() {
        int numberOfFiles = filesIdsMap.size();

        indexFilesBuilders = new Thread[numberOfFiles];
        String commonFileName = "IndexFile";
        String directoryPath = this.indexDirPath + File.separator;
        long id = 1L;

        int totalEntries = invertedIndex.size();
        int entriesPerPart = totalEntries / numberOfFiles;

        for (int i = 0; i < numberOfFiles; i++) {
            int beginIndex = i * entriesPerPart;
            int endIndex = (i + 1) * entriesPerPart;

            if (i == numberOfFiles - 1) // if lastFile -> process pending work
                endIndex = totalEntries;

            String filename = commonFileName + "_" + id++;
            List<Map.Entry<String, ArrayList<String>>> partInvertedIndex = new ArrayList<>
                    (invertedIndex.entrySet()).subList(beginIndex, endIndex);
            try{
                indexFilesBuilders[i] = Thread.startVirtualThread(new IndexFileBuilder(directoryPath, filename, partInvertedIndex));
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
    }

    /**
     * Constructs the definitive inverted index data structure that it's a result of iterating all local
     * inverted index maps.
     */
    public ConcurrentHashMap<String, ArrayList<String>> sortGlobalInvertedIndex() {
        ConcurrentHashMap<String, ArrayList<String>> invertedIndex = new ConcurrentHashMap<>();

        for (ConcurrentHashMap<String, String> map : unsortedInvertedIndex) { // iterate each map
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String word = entry.getKey();
                String locations = entry.getValue();

                if (invertedIndex.containsKey(word)) {
                    invertedIndex.get(word).add(locations);
                } else {
                    ArrayList<String> values = new ArrayList<>();
                    values.add(locations);
                    invertedIndex.put(word, values);
                }
            }
        }
        return invertedIndex;
    }

    /**
     * Rescales the fileLines map in order that the virtual threads that will construct the inverted index can
     * do the same amount of work. This helps the application to run faster.
     */
    public void constructSameSizeFileLines() {
        List<Map.Entry<Location, String>> fileLinesEntries = new ArrayList<>(fileLines.entrySet());
        int totalEntries = fileLinesEntries.size();
        int parts = filesIdsMap.size() * app.virtualThreadFactor;
        int entriesPerPart = totalEntries / parts;

        for (int i = 0; i < parts; i++) {
            int beginIndex = i * entriesPerPart;
            int endIndex = (i + 1) * entriesPerPart;

            if (i == parts - 1)
                endIndex = totalEntries;

            List<Map.Entry<Location, String>> partEntries = fileLinesEntries.subList(beginIndex, endIndex);

            ConcurrentHashMap<Location, String> partMap = new ConcurrentHashMap<>();
            for (Map.Entry<Location, String> entry : partEntries) {
                partMap.put(entry.getKey(), entry.getValue());
            }
            sameSizeFileLines.add(partMap);
        }
    }

    /**
     * Constructs the final inverted index concurrently.
     */
    public void constructInvertedIndex() {
        invertedIndexBuilders = new Thread[filesIdsMap.size() * app.virtualThreadFactor];
        invertedIndexTasks = new InvertedIndexBuilder[filesIdsMap.size() * app.virtualThreadFactor];
        int i = 0, j = 0;
        for (ConcurrentHashMap<Location, String> map : sameSizeFileLines) {
            InvertedIndexBuilder currentTask = new InvertedIndexBuilder(map);
            invertedIndexTasks[i++] = currentTask;
            try{
                invertedIndexBuilders[j++] = Thread.startVirtualThread(currentTask);
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
    }

    /**
     * Adds local tasks DS to global DS.
     */
    public void addToGlobalUnsortedInvertedIndex(){
        for (InvertedIndexBuilder task: invertedIndexTasks)
            unsortedInvertedIndex.add(task.getLocalInvertedIndex());
    }

    /**
     * Constructs the directory in which we store the resulting files.
     */
    private void mkdir() {
        File indexDirFile = new File(this.indexDirPath);
        if (!indexDirFile.exists()) {
            if (!indexDirFile.mkdirs()) {
                System.err.println("Directory: " + this.indexDirPath + " could not be created.");
            }
        }
    }

    /**
     * Deletes a file invoking recursiveDelete method.
     */
    private void rmdir() {
        File indexDir = new File(this.indexDirPath);
        if (indexDir.exists()) {
            recursiveDelete(indexDir);
        }
    }

    /**
     * Recursively deletes a file.
     *
     * @param file root file to delete recursively.
     */
    private void recursiveDelete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    recursiveDelete(child);
                }
            }
        }
        file.delete();
    }

    /**
     * For each file in inputPath a virtual thread is launched and searches for .txt files. Also removes the 'Input' dir.
     * if existed and creates it again to avoid text overriding.
     *
     * @param inputPath
     */
    public void searchTxtFiles(String inputPath) {
        rmdir();
        mkdir();
        File inputFile = new File(inputPath);
        File[] files = inputFile.listFiles();

        if (files != null) {
            SearchFiles.app = app;
            try{
                fileFinder = Thread.startVirtualThread(new SearchFiles(files));
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
    }

    /**
     * Constructs the 'FileIds' txt file which contains lines of the form [id fullTxtFilePath].
     */
    public void buildFileLinesContent() {
        linesContentVirtualThreads = new Thread[filesIdsMap.size()];// OJO 2 task
        readFilesTasks = new ReadFile[filesIdsMap.size()];
        ReadFile.app = app;
        int i = 0, j = 0;
        for (Map.Entry<Long, String> entry : filesIdsMap.entrySet()) {
            Long fileId = entry.getKey();
            String fullPath = entry.getValue();
            ReadFile currentTask = new ReadFile(fileId, new File(fullPath));
            readFilesTasks[i++] = currentTask;
            try {
                linesContentVirtualThreads[j++] = Thread.startVirtualThread(currentTask); // 2 task
            } catch (OutOfMemoryError e){
                cancel(e);
            }
        }
    }

    /**
     * Adds local tasks DS to global DS.
     */
    public void addToGlobalLinesContent(){
        for (ReadFile task: readFilesTasks){
            fileLines.putAll(task.getLocalFilesLinesContent());
        }
    }

    /**
     * Starts a virtual thread which runs the task of saving the full path of the .txt files and it's id.
     */
    public void saveFileIds() {
        SaveFileIds.app = app;
        SaveFileIds.appIndexDir = app.indexDirPath;
        try{
            fileIdSaver = Thread.startVirtualThread(new SaveFileIds());
        } catch (OutOfMemoryError e){
            cancel(e);
        }
    }

    /**
     * Joins a single thread.
     *
     * @param t
     * @throws InterruptedException
     */
    public void joinThread(Thread t) throws InterruptedException {
        t.join();
    }

    /**
     * Simply prints usage if the user has introduced wrong arguments.
     */
    private static void printUsage() {
        System.err.println("Error in parameters. At least one argument (source dir.) is needed.");
        System.err.println("Usage: Indexing <SourceDirectory> [<Index_Directory>]");
        System.exit(1);
    }

    /**
     * Generates FileLinesContent.txt file iterating fileLines global data structure.
     */
    public void constructFileLinesContent() {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.indexDirPath + File.separator +
                "FileLinesContent.txt", true))) {
            for (Map.Entry<Location, String> entry : fileLines.entrySet()) {
                bw.write(entry.getKey().toString() + " " + entry.getValue());
                bw.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Joins threads that are executing a common task.
     * @throws InterruptedException
     */
    public void joinThreads(Thread[] threads) throws InterruptedException {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void cancel(OutOfMemoryError e){
        e.printStackTrace();
        System.exit(1);
    }
}
