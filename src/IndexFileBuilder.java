/* ---------------------------------------------------------------
Práctica 1.
Código fuente: IndexFileBuilder.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexFileBuilder implements Runnable {
    private final String directoryPath;
    private final String fileName;
    List<Map.Entry<String, ArrayList<String>>> partInvertedIndex;

    public IndexFileBuilder(String directoryPath, String fileName, List<Map.Entry<String, ArrayList<String>>> partInvertedIndex){
        this.directoryPath = directoryPath;
        this.fileName = fileName;
        this.partInvertedIndex = partInvertedIndex;
    }

    /**
     * Writes part of the invertedIndex DS to a IndexFile txt.
     */
    @Override
    public void run() {
        try (BufferedWriter bw =
                     new BufferedWriter(new FileWriter(directoryPath + fileName + ".txt"))) {

            for (var entry : partInvertedIndex){
                String word = entry.getKey();
                ArrayList<String> locations = entry.getValue();
                bw.write(word + " " + locations.toString());
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}