public class DataGenerator extends Thread{

    private String zona;
    private String sensor;
    private String colecao;


    public void run(){
        new DataBridge(zona,sensor,colecao).start();
    }

    public DataGenerator(String zona, String sensor, String colecao){
        this.zona = zona;
        this.sensor=sensor;
        this.colecao=colecao;
    }

}
