import java.util.List;

public class Utilies {

    //comparador de data, dá erro se a data do doc da cloud é mais recente que o local Formato Sensores T
    public static boolean isBiggerThanSensorT(String datedoccloud, String datedoclocal){
        try {
            List datacloud= List.of(datedoccloud.split("T")[0].split("-"));
            List datalocal= List.of(datedoccloud.split("T")[0].split("-"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(2))>Integer.parseInt((String) datalocal.get(2))){
                return true;
            }
            datacloud= List.of(datedoccloud.split("T")[1].split(":"));
            datalocal= List.of(datedoccloud.split("T")[1].split(":"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) ((String) datacloud.get(2)).replace("Z",""))>Integer.parseInt((String) ((String) datalocal.get(2)).replace("Z",""))){
                return true;
            }
            return false;
        }catch (Exception e){
            System.out.println("Data No Formato Errado:"+datedoccloud);
            //logs txt?
            return false;
        }
    }
}
