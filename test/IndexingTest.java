import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

// Tests will be executed for the Big Example (All dirs in Input)
public class IndexingTest {
    String inputDir = "." + File.separator + "Input" + File.separator;
    String indexDir = "." + File.separator + "Index" + File.separator;
    Indexing app = new Indexing(inputDir, indexDir);
    @Test
    public void testDirs(){
        assertEquals(app.getAppInputDir(), inputDir);
        assertEquals(app.getAppIndexDir(), indexDir);
    }
    @Test
    public void testNumOfFiles() throws InterruptedException {
        Indexing.setApp(app);
        app.searchTxtFiles(app.getAppInputDir());
        app.joinThread(app.getFileFinder());
        app.saveFileIds();
        assertEquals(24, app.getFilesIdsMap().size());
    }

    @Test
    public void testSomeInvertedIndexKeys() throws InterruptedException{
        Indexing.setApp(app);
        app.searchTxtFiles(app.getAppInputDir());
        app.joinThread(app.getFileFinder());
        app.saveFileIds();

        app.buildFileLinesContent();
        app.joinThreads(app.getLinesContentVirtualThreads());
        app.addToGlobalLinesContent();
        app.constructFileLinesContent();
        app.constructSameSizeFileLines();

        app.constructInvertedIndex();
        app.joinThreads(app.getInvertedIndexBuilders());
        app.addToGlobalUnsortedInvertedIndex();

        var invertedIndex = app.sortGlobalInvertedIndex();

        assertTrue(invertedIndex.containsKey("de"));
        assertTrue(invertedIndex.containsKey("quijote"));
        assertTrue(invertedIndex.containsKey("mancha"));
        assertTrue(invertedIndex.containsKey("ingenioso"));
        assertTrue(invertedIndex.containsKey("don"));
        assertTrue(invertedIndex.containsKey("hidalgo"));


        assertFalse(invertedIndex.containsKey("!{[@]"));
        assertFalse(invertedIndex.containsKey("¨¨"));
        assertFalse(invertedIndex.containsKey("\\"));
    }

    @Test
    public void testDifferentLocations() throws InterruptedException{
        Indexing.setApp(app);
        app.searchTxtFiles(app.getAppInputDir());
        app.joinThread(app.getFileFinder());
        app.saveFileIds();

        app.buildFileLinesContent();
        app.joinThreads(app.getLinesContentVirtualThreads());
        app.addToGlobalLinesContent();
        app.constructFileLinesContent();
        app.constructSameSizeFileLines();

        app.constructInvertedIndex();
        app.joinThreads(app.getInvertedIndexBuilders());
        app.addToGlobalUnsortedInvertedIndex();

        var invertedIndex = app.sortGlobalInvertedIndex();

        assertTrue(invertedIndex.get("quijote").size() > 2);
        assertTrue(invertedIndex.get("ingenioso").size() > 1);
        assertTrue(invertedIndex.get("hola").size() > 1);
    }

}
