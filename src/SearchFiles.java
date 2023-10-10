/* ---------------------------------------------------------------
Práctica 1.
Código fuente: SearchFile.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.File;

public class SearchFiles implements Runnable {
    public static Indexing app;
    private final File[] files;
    private static Long fileId = 1L;
    public SearchFiles(File[] files) {
        this.files = files;
    }

    /**
     * If the file given is a txt file saves it and if it's a directory processes it recursively in
     * addToTxtFiles function.
     */
    @Override
    public void run() {
        for (File file: files){
            if (file.isFile() && isTxtFile(file)) {
                app.getFilesIdsMap().put(fileId, file.getAbsolutePath());
                fileId++;
            } else if (file.isDirectory())
                recursiveSearch(file);
        }
    }

    /**
     * Recursively searches txt files.
     * @param file
     */
    private void recursiveSearch(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        recursiveSearch(f);
                    } else if (f.isFile() && isTxtFile(f)) {
                        app.getFilesIdsMap().put(fileId, f.getAbsolutePath());
                        fileId++;
                    }
                }
            }
        }
    }

    /**
     * Simply returns if a file is a txt file or not.
     * @param file
     */
    private static boolean isTxtFile(File file) {
        return file.getName().endsWith("txt");
    }
}

