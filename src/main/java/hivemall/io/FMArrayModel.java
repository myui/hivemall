package hivemall.io;

import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import hivemall.common.EtaEstimator;
import hivemall.io.FactorizationMachineModel;
import hivemall.mf.FactorizationMachineUDTF.Feature;
import hivemall.utils.math.MathUtils;

public class FMArrayModel implements FactorizationMachineModel {
	
	// PRE DEFINED VARIABLES
	private enum ETA_UPDATE{fix, time, powerTime, ada};
	private static float power_t = 0.01f;
	private static int total_steps = 1;
	private static long SEED = 11111111;
	
	// LEARNING PARAMS
	private float w0;
	private float[] w;
	private float[][] V;
	
	// Regulation Variables
	private float lambdaW0;
	private float lambdaW;
	private float lambdaV;
	
	// Eta
	EtaEstimator eta;
	
	// Random
	private Random rnd;
	
	
	private boolean classification;
	private int factor;
	private float eta0;
	private int[] x_group;
	private float sigma;
	private String etaUpdateMethod = "fix";
	private int col;

	public FMArrayModel(boolean classification, int factor, float lambda0, float eta0, int[] x_group, float sigma,
			String etaUpdateMethod, int col) {
		this.classification = classification;
		this.factor = factor;
		this.eta0 = eta0;
		this.x_group = x_group;
		this.sigma = sigma;
		this.etaUpdateMethod = etaUpdateMethod;
		this.col = col;
		
		// TODO remove
		try {
			throw new HiveException("ARRAY MODEL CALLED");
		} catch (HiveException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Initialize
		initRandom();
		initLearningParams();
		initLambdas(lambda0);
		try {
			initEta(eta0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void initRandom() {
		rnd = new Random();
		rnd.setSeed(SEED);
	}

	private void initEta(float eta0) throws Exception {
		if(etaUpdateMethod.equals(ETA_UPDATE.fix.toString())){
			eta = new EtaEstimator.FixedEtaEstimator(eta0);
		}else if(etaUpdateMethod.equals(ETA_UPDATE.time.toString())){
			eta = new EtaEstimator.InvscalingEtaEstimator(eta0, power_t);
		}else if(etaUpdateMethod.equals(ETA_UPDATE.powerTime.toString())){
			eta = new EtaEstimator.SimpleEtaEstimator(eta0, total_steps);
		}else if(etaUpdateMethod.equals(ETA_UPDATE.ada.toString())){
			// TODO ada eta
		}else{
			throw new Exception("EtaUpdateMethod");
		}
	}

	private void initLambdas(float lambda0) {
		this.lambdaW0 = lambda0;
		this.lambdaW  = lambda0;
		this.lambdaV  = lambda0;
	}

	private void initLearningParams() {
		w0 = 0f;
		w = new float[col];
		Arrays.fill(w, 0f);
		V = new float[col][factor];
		for(int i=0; i<col; i++){
			for(int j=0; j<factor; j++){
				V[i][j] = (float) MathUtils.gaussian(0.d, (double)sigma, rnd);
			}
		}
	}

	@Override
	public float getW(int i) {
		if(i==0){
			return w0;
		}else{
			return w[i-1];
		}
	}

	@Override
	public float getV(int i, int f) {
		return V[i][f];
	}

	@Override
	public void updateW0(Feature[] x, double y, long t) {
		float grad0 = gLoss0(x, (float)y);
		float nextW0 = w0 - eta.eta(t) * (grad0 + 2 * lambdaW0 * w0);
		setW0(nextW0);
	}

	private void setW0(float nextW0) {
		this.w0 = nextW0;
	}

	private float gLoss0(Feature[] x, float y) {
		float ret = -1f;

		float predict0 = predict(x, y);

		if(!classification){
			float diff = predict0 - y;
			ret = 2 * diff * 1;
		}else{
			ret = (float) (MathUtils.sigmoid(predict0 * y) - 1) * y * 1;
		}
		return ret;
	}
	
	public float predict(Feature[] x, double y) {
		// For Training
		double ret = 0;
		// w0
		ret += w0;
		
		// W
		for(Feature e:x){
			int j = e.index;
			float xj = (float) e.value;
			ret += w[j] * xj;
		}
		
		// V
		for(int f=0, k=factor; f<k; f++){
			float sumVjfXj = 0f;
			float sumV2X2 = 0f;
			
			for(Feature e:x){
				int j = e.index;
				float xj = (float) e.value;
				float vjf= V[j][f];
				sumVjfXj += vjf * xj;
				sumV2X2  += (vjf * vjf * xj * xj);
			}
			sumVjfXj *= sumVjfXj;
			ret += 0.5 * (sumVjfXj - sumV2X2);
			if(Double.isNaN(ret)){
				System.out.print("");
				System.exit(1);
			}
		}
//		System.out.println(ret);
		if(classification){
			if(y == 1.0){
				ret = (float) MathUtils.sigmoid(ret);
			}else{
				ret = 1 - (float)MathUtils.sigmoid(ret);
			}
			if(ret > 0.5){
				ret = 0f;
			}else{
				ret = 1f;
			}
		}
		if(Double.isNaN(ret)){
			System.out.println(ret);
			System.exit(1);
		}
		return (float)ret;
	}
	
	@Override
	public float predict(Feature[] x) {
		double ret = 0;
		// w0
		ret += w0;
		
		// W
		for(Feature e:x){
			int j = e.index;
			float xj = (float) e.value;
			ret += w[j] * xj;
		}
		
		// V
		for(int f=0, k=factor; f<k; f++){
			float sumVjfXj = 0f;
			float sumV2X2 = 0f;
			
			for(Feature e:x){
				int j = e.index;
				float xj = (float) e.value;
				float vjf= V[j][f];
				sumVjfXj += vjf * xj;
				sumV2X2  += (vjf * vjf * xj * xj);
			}
			sumVjfXj *= sumVjfXj;
			ret += 0.5 * (sumVjfXj - sumV2X2);
			if(Double.isNaN(ret)){
				System.out.print("");
				System.exit(1);
			}
		}
//		System.out.println(ret);
		if(classification){
			ret = (float) MathUtils.sigmoid(ret);
			if(ret > 0.5){
				ret = 1f;
			}else{
				ret = 0f;
			}
		}
		if(Double.isNaN(ret)){
			System.out.println(ret);
			System.exit(1);
		}
		return (float)ret;
	}

	@Override
	public void updateWi(Feature[] x, double y, int i, float xi, long t) {
		float gradWi = gLossWi(x, y, xi);
		float wi = w[i];
		float nextWi = wi - eta.eta(t) * (gradWi + 2 * lambdaW * wi);
		setWi(i, nextWi);
	}

	private void setWi(int i, float nextWi) {
		w[i] = nextWi;
	}

	private float gLossWi(Feature[] x, double y, float xi) {
		float ret = -1;
		float predictWi = predict(x, y);
		if(!classification){
			float diff = predictWi - (float) y;
			ret = 2 * diff * xi;
		}else{
			ret = (float) ((MathUtils.sigmoid(predictWi * y) - 1) * y * xi);
		}
		return ret;
	}

	@Override
	public void updateV(Feature[] x, double y, int i, int f, long t) {
		float gradV = gLossV(x, y, i, f);
		float vif = V[i][f];
		float nextVif = vif - eta.eta(t) * (gradV + 2 * lambdaV * vif);
		//System.out.println("vif:" + vif + " nextVif:" + nextVif);
		setVif(i, f, nextVif);
	}

	private void setVif(int i, int f, float nextVif) {
		V[i][f] = nextVif;
	}

	private float gLossV(Feature[] x, double y, int i, int f) {
		float ret = -1f;
		float predictV = predict(x, y);
		if(!classification){
			float diff = predictV - (float) y;
			ret = 2 * diff * gradV(x, i, f);
		}else{
			ret = (float)((MathUtils.sigmoid(predictV * y) - 1) * y * gradV(x, i, f));
		}
		return ret;
	}

	private float gradV(Feature[] x, int i, int f) {
		float ret = 0f;
		float xi = 1f;
		for(Feature e:x){
			int j = e.index;
			float xj= (float) e.value;
			
			if(j == i){
				xi = xj;
				continue;
			}else{
				ret += V[j][f] * xj;
			}	
		}
		ret *= xi;
		return ret;
	}

	@Override
	public void check(Feature[] xx) {
		// do nothing 
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		int size = this.col;
		return size;
	}

}