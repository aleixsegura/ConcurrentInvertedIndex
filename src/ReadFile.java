/* ---------------------------------------------------------------
Práctica 1.
Código fuente: ReadFile.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadFile implements Runnable {
    public static Indexing app;
    private static final AtomicInteger localMapsRead = new AtomicInteger();
    private final Long fileId;
    private final File fileToRead;
    private final ConcurrentHashMap<Location, String> filesLinesContent;

    public ReadFile(Long fileId, File fileToRead){
        this.fileId = fileId;
        this.fileToRead = fileToRead;
        filesLinesContent = new ConcurrentHashMap<>();
    }

    /**
     * Simply adds the local fileLinesContent map to global fileLines.
     */
    @Override
    public void run(){
        try (BufferedReader br = new BufferedReader(new FileReader(fileToRead))){
            String line;
            Long lineNumber = 1L;
            while ((line = br.readLine()) != null){
                Location location = new Location(fileId, lineNumber);
                filesLinesContent.put(location, line);
                lineNumber++;
            }

            synchronized (app.getFileLines()){ // add to global DS
                app.getFileLines().putAll(filesLinesContent);
            }
            localMapsRead.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Simply returns the amount of localMaps read.
     */
    public static AtomicInteger getLocalMapsRead() { return localMapsRead; }
}
