/* ---------------------------------------------------------------
Práctica 1.
Código fuente: Location.java
Grau Informàtica
Aleix Segura Paz.
Aniol Serrano Ortega.
--------------------------------------------------------------- */
public class Location{
    private final Long fileId;
    private final Long fileLine;

    public Location(Long fileId, Long fileLine){
        this.fileId = fileId;
        this.fileLine = fileLine;
        String location = this.toString();
    }

    @Override
    public String toString(){
        return "(" + fileId.toString() + ", " + fileLine.toString() + ")";
    }


}
