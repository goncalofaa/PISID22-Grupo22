
public class Cultura {
	
	private int idCultura;
	private int idUtilizador;
	private double limiteLuzMax;
	private double limiteLuzMin;
	private double limiteHumMax;
	private double limiteHumMin;
	private double limiteTempMax;
	private double limiteTempMin;
	private double avisoLuzMax;
	private double avisoLuzMin;
	private double avisoHumMax;
	private double avisoHumMin;
	private double avisoTempMax;
	private double avisoTempMin;
	private int intervaloAviso;
	
	public Cultura() {
		super();
	}
	
	

	public Cultura(int idCultura, int idUtilizador, double limiteLuzMax, double limiteLuzMin, double limiteHumMax,
			double limiteHumMin, double limiteTempMax, double limiteTempMin, double avisoLuzMax, double avisoLuzMin,
			double avisoHumMax, double avisoHumMin, double avisoTempMax, double avisoTempMin, int intervaloAviso) {
		super();
		this.idCultura = idCultura;
		this.idUtilizador = idUtilizador;
		this.limiteLuzMax = limiteLuzMax;
		this.limiteLuzMin = limiteLuzMin;
		this.limiteHumMax = limiteHumMax;
		this.limiteHumMin = limiteHumMin;
		this.limiteTempMax = limiteTempMax;
		this.limiteTempMin = limiteTempMin;
		this.avisoLuzMax = avisoLuzMax;
		this.avisoLuzMin = avisoLuzMin;
		this.avisoHumMax = avisoHumMax;
		this.avisoHumMin = avisoHumMin;
		this.avisoTempMax = avisoTempMax;
		this.avisoTempMin = avisoTempMin;
		this.intervaloAviso = intervaloAviso;
	}

	public double getLimiteLuzMax() {
		return limiteLuzMax;
	}

	public void setLimiteLuzMax(double limiteLuzMax) {
		this.limiteLuzMax = limiteLuzMax;
	}

	public double getLimiteLuzMin() {
		return limiteLuzMin;
	}

	public void setLimiteLuzMin(double limiteLuzMin) {
		this.limiteLuzMin = limiteLuzMin;
	}

	public double getLimiteHumMax() {
		return limiteHumMax;
	}

	public void setLimiteHumMax(double limiteHumMax) {
		this.limiteHumMax = limiteHumMax;
	}

	public double getLimiteHumMin() {
		return limiteHumMin;
	}

	public void setLimiteHumMin(double limiteHumMin) {
		this.limiteHumMin = limiteHumMin;
	}

	public double getLimiteTempMax() {
		return limiteTempMax;
	}

	public void setLimiteTempMax(double limiteTempMax) {
		this.limiteTempMax = limiteTempMax;
	}

	public double getLimiteTempMin() {
		return limiteTempMin;
	}

	public void setLimiteTempMin(double limiteTempMin) {
		this.limiteTempMin = limiteTempMin;
	}

	public double getAvisoLuzMax() {
		return avisoLuzMax;
	}

	public void setAvisoLuzMax(double avisoLuzMax) {
		this.avisoLuzMax = avisoLuzMax;
	}

	public double getAvisoLuzMin() {
		return avisoLuzMin;
	}

	public void setAvisoLuzMin(double avisoLuzMin) {
		this.avisoLuzMin = avisoLuzMin;
	}

	public double getAvisoHumMax() {
		return avisoHumMax;
	}

	public void setAvisoHumMax(double avisoHumMax) {
		this.avisoHumMax = avisoHumMax;
	}

	public double getAvisoHumMin() {
		return avisoHumMin;
	}

	public void setAvisoHumMin(double avisoHumMin) {
		this.avisoHumMin = avisoHumMin;
	}

	public double getAvisoTempMax() {
		return avisoTempMax;
	}

	public void setAvisoTempMax(double avisoTempMax) {
		this.avisoTempMax = avisoTempMax;
	}

	public double getAvisoTempMin() {
		return avisoTempMin;
	}

	public void setAvisoTempMin(double avisoTempMin) {
		this.avisoTempMin = avisoTempMin;
	}

	public int getIntervaloAviso() {
		return intervaloAviso;
	}

	public void setIntervaloAviso(int intervaloAviso) {
		this.intervaloAviso = intervaloAviso;
	}

	public int getIdCultura() {
		return idCultura;
	}

	public void setIdCultura(int idCultura) {
		this.idCultura = idCultura;
	}

	public int getIdUtilizador() {
		return idUtilizador;
	}

	public void setIdUtilizador(int idUtilizador) {
		this.idUtilizador = idUtilizador;
	}
	
}
