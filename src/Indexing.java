/* ---------------------------------------------------------------
Práctica 1.
Código fuente: Indexing.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Indexing {
    private static int VIRTUAL_THREAD_FACTOR = 150; // multiplies this value to number of files in order to create more Virtual Threads.
    private static InvertedIndex app;
    public static String appInputDir;
    public static String appIndexDir;
    public static final ConcurrentHashMap<Long, String> filesIdsMap = new ConcurrentHashMap<>(); // filesIds Data Structure
    public static final ConcurrentHashMap<Location, String> fileLines = new ConcurrentHashMap<>(); // fileLines Data Structure
    static List<Map.Entry<Location, String>> fileLinesEntries;
    private static ConcurrentHashMap<String, ArrayList<String>> invertedIndex = new ConcurrentHashMap<>(); // invertedIndex Data Structure

    private static final ArrayList<ConcurrentHashMap<Location, String>> sameSizeFileLines = new ArrayList<>(); // Rescaled in order to force each virtual thread does the same work.

    private static Thread[] searchFilesVirtualThreads;
    private static Thread[] linesContentVirtualThreads;
    private static Thread[] invertedIndexBuilders;
    private static Thread fileIdSaver;
    private static Thread[] indexFilesBuilders;


    public static void main(String[] args) throws InterruptedException {
        if (args.length == 4){
            app = new InvertedIndex(args[1], args[2]);
            VIRTUAL_THREAD_FACTOR = Integer.parseInt(args[3]);
        }
        else if (args.length == 3) // args[0] is --enable-preview for Virtual Threads
            app = new InvertedIndex(args[1], args[2]);
        else if (args.length == 2)
            app = new InvertedIndex(args[1]);
        else
            printUsage();

        appInputDir = app.getInputDirPath();
        appIndexDir = app.getIndexDirPath();
        Instant start = Instant.now();

        searchTxtFiles(appInputDir);
        joinThreads(searchFilesVirtualThreads);
        saveFileIds();
        joinThread(fileIdSaver);


        buildFileLinesContent();
        joinThreads(linesContentVirtualThreads);
        constructFileLinesContent();

        constructSameSizeFileLines();

        constructInvertedIndex();
        joinThreads(invertedIndexBuilders);
        invertedIndex = sortGlobalInvertedIndex();

        generateInvertedIndexFiles();
        joinThreads(indexFilesBuilders);


        Instant end = Instant.now();
        System.out.println("EXECUTION TIME: " + Duration.between(start, end).toMillis() + " milliseconds.");
    }


    private static void generateInvertedIndexFiles(){
        int numberOfFiles = filesIdsMap.size();

        indexFilesBuilders = new Thread[numberOfFiles];
        String commonFileName = "IndexFile";
        String directoryPath = appIndexDir  + File.separator;
        Long id = 1L;

        int totalEntries = invertedIndex.size();
        int entriesPerPart = totalEntries / numberOfFiles;

        for (int i = 0; i < numberOfFiles; i++){
            int beginIndex = i * entriesPerPart;
            int endIndex = (i + 1) * entriesPerPart;

            if (i == numberOfFiles - 1) // if lastFile -> process pending work
                endIndex = totalEntries;

            String filename = commonFileName + "_" + id++;
            List<Map.Entry<String, ArrayList<String>>> partInvertedIndex = new ArrayList<>
                                                (invertedIndex.entrySet()).subList(beginIndex, endIndex);

            indexFilesBuilders[i] = Thread.startVirtualThread(new IndexFileBuilder(directoryPath, filename, partInvertedIndex));
        }
    }

    /**
     * Constructs the definitive inverted index data structure that it's a result of iterating all local
     * inverted index maps.
     */
    private static ConcurrentHashMap<String, ArrayList<String>> sortGlobalInvertedIndex(){
        ConcurrentHashMap<String, ArrayList<String>> invertedIndex = new ConcurrentHashMap<>();

        for (HashMap<String, String> map : InvertedIndexBuilder.globalInvertedIndex) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (invertedIndex.containsKey(key)) {
                    invertedIndex.get(key).add(value);
                } else {
                    ArrayList<String> values = new ArrayList<>();
                    values.add(value);
                    invertedIndex.put(key, values);
                }
            }
        }
        return invertedIndex;
    }

    /**
     * Rescales the fileLines map in order that the virtual threads that will construct the inverted index can
     * do the same amount of work. This helps the application to run faster.
     */
    private static void constructSameSizeFileLines(){
        fileLinesEntries = new ArrayList<>(fileLines.entrySet());
        int totalEntries = fileLinesEntries.size();
        int parts = filesIdsMap.size() * VIRTUAL_THREAD_FACTOR;
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
    private static void constructInvertedIndex(){
        invertedIndexBuilders = new Thread[filesIdsMap.size() * VIRTUAL_THREAD_FACTOR];

        int i = 0;
        for (ConcurrentHashMap<Location, String> map: sameSizeFileLines){
            invertedIndexBuilders[i++] = Thread.startVirtualThread(new InvertedIndexBuilder(map));
        }
    }

    /**
     * Constructs the directory in which we store the resulting files.
     */
    private static void mkdir(){
        File indexDirFile = new File(appIndexDir);
        if (!indexDirFile.exists()) {
            if (!indexDirFile.mkdirs()) {
                System.err.println("Directory: " + appIndexDir + " could not be created.");
            }
        }
    }

    /**
     * Deletes a file invoking recursiveDelete method.
     */
    private static void rmdir() {
        File indexDir = new File(appIndexDir);
        if (indexDir.exists()) {
            recursiveDelete(indexDir);
        }
    }

    /**
     * Recursively deletes a file.
     * @param file root file to delete recursively.
     */
    private static void recursiveDelete(File file) {
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
     * @param inputPath
     */
    private static void searchTxtFiles(String inputPath) {
        rmdir();
        mkdir();
        File inputFile = new File(inputPath);
        File [] files = inputFile.listFiles();

        if (files != null) {
            searchFilesVirtualThreads = new Thread[files.length];
            for (int i = 0; i < searchFilesVirtualThreads.length; i++){
                searchFilesVirtualThreads[i] = Thread.startVirtualThread(new SearchFile(files[i]));
            }
        }
    }

    /**
     * Constructs the 'FileIds' txt file which contains lines of the form [id fullTxtFilePath].
     */
    private static void buildFileLinesContent() {
        linesContentVirtualThreads = new Thread[filesIdsMap.size()];// OJO 2 task
        int i = 0;
        for (Map.Entry<Long, String> entry : filesIdsMap.entrySet()){
            Long fileId = entry.getKey();
            String fullPath = entry.getValue();
            linesContentVirtualThreads[i++] = Thread.startVirtualThread(new ReadFile(fileId, new File(fullPath))); // 2 task
        }
    }


    /**
     * Starts a virtual thread which runs the task of saving the full path of the .txt files and it's id.
     */
    private static void saveFileIds(){ fileIdSaver = Thread.startVirtualThread(new SaveFileIds());}

    /**
     * Joins a single thread.
     * @param t
     * @throws InterruptedException
     */
    private static void joinThread(Thread t) throws InterruptedException { t.join(); }

    /**
     * Simply prints usage if the user has introduced wrong arguments.
     */
    private static void printUsage(){
        System.err.println("Error in parameters. At least one argument (source dir.) is needed.");
        System.err.println("Usage: Indexing <SourceDirectory> [<Index_Directory>]");
        System.exit(1);
    }

    /**
     * Generates FileLinesContent.txt file iterating fileLines global data structure.
     */
    private static void constructFileLinesContent(){
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(Indexing.appIndexDir + File.separator +
                "FileLinesContent.txt", true))){
            for (Map.Entry<Location, String> entry: fileLines.entrySet()){
                bw.write(entry.getKey().toString()+ " " + entry.getValue());
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
    private static void joinThreads(Thread[] threads) throws InterruptedException{
        for (Thread t: threads){
            try{
                t.join();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
