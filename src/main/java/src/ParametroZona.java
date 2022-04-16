package src;
public class ParametroZona {

    private int IDzona;
    private double MargemOutlierTemp;
    private double MargemOutlierHum;
    private double MargemOutlierLum;

    public ParametroZona() {}

    public ParametroZona(int iDzona, double margemOutlierTemp, double margemOutlierHum, double margemOutlierLum) {
        super();
        IDzona = iDzona;
        MargemOutlierTemp = margemOutlierTemp;
        MargemOutlierHum = margemOutlierHum;
        MargemOutlierLum = margemOutlierLum;
    }

    public int getIDzona() {
        return IDzona;
    }

    public void setIDzona(int iDzona) {
        IDzona = iDzona;
    }

    public double getMargemOutlierTemp() {
        return MargemOutlierTemp;
    }

    public void setMargemOutlierTemp(double margemOutlierTemp) {
        MargemOutlierTemp = margemOutlierTemp;
    }

    public double getMargemOutlierHum() {
        return MargemOutlierHum;
    }

    public void setMargemOutlierHum(double margemOutlierHum) {
        MargemOutlierHum = margemOutlierHum;
    }

    public double getMargemOutlierLum() {
        return MargemOutlierLum;
    }

    public void setMargemOutlierLum(double margemOutlierLum) {
        MargemOutlierLum = margemOutlierLum;
    }

}
