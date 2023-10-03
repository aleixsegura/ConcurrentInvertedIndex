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
    static final AtomicLong readFiles = new AtomicLong(0);
    private static final AtomicLong mapIndex = new AtomicLong(0);
    private final File file;
    private final ConcurrentLinkedQueue<String> txtFilesPaths;

    public SearchFile(File file) {
        this.file = file;
        this.txtFilesPaths = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void run() {
        if (file.isFile() && isTxtFile(file)) {
            txtFilesPaths.offer(file.getAbsolutePath());
            readFiles.incrementAndGet();
        } else
            addToTxtFiles(file);
        addToGlobalMap(txtFilesPaths);
    }


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

    private static boolean isTxtFile(File f) {
        return f.getName().endsWith("txt");
    }


    private static void addToGlobalMap(ConcurrentLinkedQueue<String> txtFilesPaths) {
        long key;
        synchronized (Indexing.filesIdsMap) {
            while (!txtFilesPaths.isEmpty()) {
                key = mapIndex.incrementAndGet();
                String value = txtFilesPaths.poll();
                assert value != null;
                Indexing.filesIdsMap.put(key, value);

            }
        }
    }

    public static AtomicLong getReadFiles(){ return readFiles; }

}

