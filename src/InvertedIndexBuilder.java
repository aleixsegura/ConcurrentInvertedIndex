/* ---------------------------------------------------------------
Práctica 1.
Código fuente: InvertedIndexBuilder.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InvertedIndexBuilder implements Runnable {
    private final ConcurrentHashMap<Location, String> localFileLinesMap;
    private final HashMap<String, String> localInvertedIndex;
    static final ArrayList<HashMap<String, String>> globalInvertedIndex = new ArrayList<>();

    public InvertedIndexBuilder(ConcurrentHashMap<Location, String> localFileLinesMap) {
        this.localFileLinesMap = localFileLinesMap;
        localInvertedIndex = new HashMap<>();
    }

    /**
     * For each word in each entry of the localMap we build the inverted index checking if the location is already in
     * the local inverted index or not. Then we add the localInvertedIndex to the globalInvertedIndex.
     */
    @Override
    public void run() {
        for (Map.Entry<Location, String> entry : localFileLinesMap.entrySet()) {
            Location location = entry.getKey();
            String toStringLocation = location.toString();
            String line = entry.getValue();

            String[] words = line.split("\\s+");
            StringBuilder locationsBuilder = new StringBuilder(toStringLocation);

            for (String word : words) {
                word = word.toLowerCase();
                word = word.replaceAll("[^a-zA-Z0-9]", "");

                localInvertedIndex.merge(word, locationsBuilder.toString(), (existing, newLocations) ->
                        existing.contains(toStringLocation) ? existing : newLocations + " " + existing);

            }
        }
        synchronized (globalInvertedIndex){
            globalInvertedIndex.add(localInvertedIndex);
        }
    }

}






