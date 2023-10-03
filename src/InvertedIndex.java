/* ---------------------------------------------------------------
Práctica 1.
Código fuente: InvertedIndex.java
Grau Informàtica
48056540H - Aleix Segura Paz.
21161168H - Aniol Serrano Ortega.
--------------------------------------------------------------- */


import java.io.File;

public class InvertedIndex {
    private final String DefaultIndexDir = "." + File.separator + "Index" + File.separator;

    // Members
    private final String inputDirPath;
    private final String indexDirPath;

    public InvertedIndex(String inputDirPath, String indexDirPath){
        this.inputDirPath = inputDirPath;
        this.indexDirPath = indexDirPath;
    }

    public InvertedIndex(String inputDirPath){
        this.inputDirPath = inputDirPath;
        this.indexDirPath = DefaultIndexDir;
    }

    // Getters
    public String getInputDirPath() { return inputDirPath; }
    public String getIndexDirPath() { return indexDirPath; }

}
