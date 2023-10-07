/* ---------------------------------------------------------------
Práctica 1.
Código fuente: SearchFile.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */
import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class SearchFile implements Runnable {
    public static Indexing app;
    static final AtomicLong readFiles = new AtomicLong(0);
    private static final AtomicLong mapIndex = new AtomicLong(0);
    private final File file;
    private final ConcurrentLinkedQueue<String> txtFilesPaths;

    public SearchFile(File file) {
        this.file = file;
        this.txtFilesPaths = new ConcurrentLinkedQueue<>();
    }

    /**
     * If the file given is a txt file saves it and if it's a directory processes it recursively in
     * addToTxtFiles function.
     */
    @Override
    public void run() {
        if (file.isFile() && isTxtFile(file)) {
            txtFilesPaths.offer(file.getAbsolutePath());
            readFiles.incrementAndGet();
        } else
            addToTxtFiles(file);
        addToGlobalMap(txtFilesPaths);
    }


    /**
     * Recursively searches txt files.
     * @param file
     */
    private void addToTxtFiles(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        addToTxtFiles(f);
                    } else if (f.isFile() && isTxtFile(f)) {
                        txtFilesPaths.offer(f.getAbsolutePath());
                        readFiles.incrementAndGet();
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

    /**
     * Adds in a synchronized way the txt the identifiers and the full paths of the txt files encountered by the
     * thread.
     * @param txtFilesPaths
     */
    private static void addToGlobalMap(ConcurrentLinkedQueue<String> txtFilesPaths) {
        long key;
        synchronized (app.getFilesIdsMap()) {
            while (!txtFilesPaths.isEmpty()) {
                key = mapIndex.incrementAndGet();
                String value = txtFilesPaths.poll();
                assert value != null;
                app.getFilesIdsMap().put(key, value);
            }
        }
    }

    /**
     * Simply return the amount of txt files.
     * @return
     */
    public static AtomicLong getReadFiles() { return readFiles;}
}

