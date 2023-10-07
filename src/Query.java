/* ---------------------------------------------------------------
Práctica 1.
Código fuente: Query.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */


import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Query {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_GREEN_YELLOW_UNDER = "\u001B[32;40;4m";
    private static final float DMatchingPercentage = 0.80f;
    private static final String indexFilesPrefix = "IndexFile_";
    private static String[] wordsToQuery;
    private static String indexPath;
    private static final ConcurrentHashMap<String, Integer> queryMatchings = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, String> invertedIndex = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        if (args.length < 3)
            printUsage();

        getWordsToQuery(args);
        indexPath = args[args.length - 1];

        loadInvertedIndex();
        analyzeQueryResults();
    }

    /**
     * Loads the invertedIndex to the invertedIndex DS by reading from the IndexFiles in memory.
     */
    private static void loadInvertedIndex(){
        File indexDir = new File(indexPath);
        File[] listOfFiles = indexDir.listFiles((d, name) -> name.startsWith(indexFilesPrefix));
        if (listOfFiles == null){
            System.out.println("No Index Files we're found.");
            System.exit(-1);
        }

        for (File file : listOfFiles) {
            if (file.isFile()) {
                try {
                    FileReader input = new FileReader(file);
                    BufferedReader bufRead = new BufferedReader(input);
                    String line;
                    try {
                        while ( (line = bufRead.readLine()) != null) {
                            String cleanLine = line.replaceAll("[\\[\\]]", "");
                            String[] lineParts = cleanLine.split("\\(", 2);
                            String word = lineParts[0].trim();
                            String locations = "(" + lineParts[1];
                            invertedIndex.put(word, locations);
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading Index file");
                        e.printStackTrace();
                    }
                } catch (FileNotFoundException e) {
                    System.err.println("Error opening Index file");
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * Analyzes and gets the results of the query we introduced in relation to our txt files.
     */
    private static void analyzeQueryResults(){
        int querySize = wordsToQuery.length;

        for (String word: wordsToQuery){
            if (invertedIndex.get(word) == null)
                continue;

            String locations = invertedIndex.get(word);
            String cleanLocations = locations.replaceAll("\\),\\s*\\(", ") (");

            String[] locationsArray = cleanLocations.split("\\(");
            for (int i = 1; i < locationsArray.length; i++) { // Skip first location which is "(" due to split with regex
                locationsArray[i] = "(" + locationsArray[i];
                Integer value = queryMatchings.putIfAbsent(locationsArray[i], 1);
                if (value != null){
                    queryMatchings.put(locationsArray[i], value + 1);
                }
            }
        }

        if (queryMatchings.size() == 0)
            System.out.printf(ANSI_RED + "No matchings found.\n" + ANSI_RESET);
        else
            for (Map.Entry<String, Integer> match: queryMatchings.entrySet()){
                String location = match.getKey();
                String fileId = location.substring(1, location.indexOf(',')).trim();
                String line = location.substring(location.indexOf(',') + 1, location.indexOf(')')).trim();
                if ((match.getValue()/(float)querySize)==1.0)
                    System.out.printf(ANSI_GREEN_YELLOW_UNDER+"%.2f%% Full Matching found in line %s of file %s: %s.\n"+ANSI_RESET,(match.getValue()/(float)querySize)*100.0, line, fileId, getLine(fileId, line));
                else if ((match.getValue()/(float) querySize) >= DMatchingPercentage){
                    System.out.printf(ANSI_GREEN+"%.2f%% Matching found in line %s of file %s: %s.\n" + ANSI_RESET, (match.getValue()/(float)querySize)*100.0, line, fileId, getLine(fileId, line));
                }
            }

    }

    /**
     * Obtains each work that form the query and introduces it in to an array.
     * @param args
     */
    private static void getWordsToQuery(String[] args){
        wordsToQuery = new String[args.length - 2];
        for (int i = 0; i < wordsToQuery.length; i++){
            wordsToQuery[i] = args[i+1].toLowerCase();
        }
    }

    /**
     * Simply prints correct usage.
     */
    private static void printUsage(){
        System.err.println("Error in parameters. At least two arguments are needed.");
        System.err.println("Usage: Query <word1> [<word2> ... <wordN>] <Index_Directory>");
        System.exit(1);
    }

    /**
     * Simply returns the full path of a file given its identifier in FileIds.txt file.
     * @param id
     */
    private static String getNameOfFile(String id){
        File fileIds = new File(indexPath + File.separator + "FileIds.txt");
        try {
            FileReader input = new FileReader(fileIds);
            BufferedReader bufRead = new BufferedReader(input);
            String line;
            while ((line = bufRead.readLine()) != null){
                String fileId = line.substring(0, line.indexOf(' '));
                String filePath = line.substring(line.indexOf(' ') + 1);
                if (fileId.equals(id)){
                    return filePath;
                }
            }
        }
        catch (IOException e){
            System.err.println("Error opening Index file");
            e.printStackTrace();
        }
        return "";
    }

    /**
     * Simply returns the content of a line given a file identifier and a line number.
     * @param fileId
     * @param lineNumber
     */
    private static String getLine(String fileId, String lineNumber){
        String fileToOpenPath = getNameOfFile(fileId);
        try {
            FileReader input = new FileReader(fileToOpenPath);
            BufferedReader bufRead = new BufferedReader(input);
            int currentLineNumber = 1;
            String line;
            while ((line = bufRead.readLine()) != null){
                if (Integer.parseInt(lineNumber) == currentLineNumber){
                    return line;
                }
                currentLineNumber++;
            }
        }
        catch (IOException e){
            System.err.println("Error opening Index file");
            e.printStackTrace();
        }
        return "";
    }


}
