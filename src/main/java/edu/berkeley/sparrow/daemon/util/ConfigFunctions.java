package edu.berkeley.sparrow.daemon.util;

import org.apache.commons.math3.distribution.UniformRealDistribution;

import java.io.IOException;
import java.util.ArrayList;

public class ConfigFunctions {
    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    //Gets index  where cdf allows retrieving index having higher workerspeed with higher probability
    public static int getIndexFromPSS(double[] cdf_worker_speed, ArrayList<Integer> workerIndex){
        UniformRealDistribution uniformRealDistribution = new UniformRealDistribution();
        int workerIndexReservation= java.util.Arrays.binarySearch(cdf_worker_speed, uniformRealDistribution.sample());
        if (workerIndexReservation < 0) {
            workerIndexReservation = Math.abs(workerIndexReservation) - 1;
        } else {
            workerIndexReservation = Math.abs(workerIndexReservation);
        }
        //This doesn't allow probing the same nodemonitor twice
//        if(workerIndex.contains(workerIndexReservation)){
//            workerIndexReservation = getIndexFromPSS(cdf_worker_speed, workerIndex);
//        }
        return workerIndexReservation;
    }

    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    public static double[] getCDFWokerSpeed(ArrayList<Double> workerSpeedList) throws IOException {

        //Gets the CDF of workers Speed
        double sum = 0;
        for(double d : workerSpeedList)
            sum += d;

        double[] cdf_worker_speed = new double[workerSpeedList.size()];
        double cdf = 0;
        int j = 0;
        for (double d: workerSpeedList){
            d = d/sum;
            cdf= cdf+ d;
            cdf_worker_speed[j] = cdf;
            j++;
        }
        //CDF of worker speed + ConfigFunctions based on Qiong's python pss file
        return cdf_worker_speed;
    }


    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    //Gets index  where cdf allows retrieving index having higher workerspeed with higher probability
    public static double halo(ArrayList<Double> workerSpeed, double currentWorkerSpeed, double load) {
        double sumSquareRoot = 0;
        double sum = 0;

        for (double w : workerSpeed) {
            sum = sum + w;
            sumSquareRoot = sumSquareRoot + Math.sqrt(w);
        }
        double lambda = (sum) * load;
        double sqrtCurrentWorkerIndex = Math.sqrt(currentWorkerSpeed);
        double haloOutput = ((currentWorkerSpeed * sumSquareRoot) - (sqrtCurrentWorkerIndex * sum) + (lambda * sqrtCurrentWorkerIndex)) / (lambda * sumSquareRoot);
        //Converts negative output to 0
        if (haloOutput <= 0) {
            haloOutput = 0;
        }
        return haloOutput;
    }

    public static int getIndexHalo(double[] cdf_worker_speed, ArrayList<Integer> workerIndex) {
        UniformRealDistribution uniformRealDistribution = new UniformRealDistribution();
        int workerIndexReservation = java.util.Arrays.binarySearch(cdf_worker_speed, uniformRealDistribution.sample());
        if (workerIndexReservation < 0) {
            workerIndexReservation = Math.abs(workerIndexReservation) - 1;
        } else {
            workerIndexReservation = Math.abs(workerIndexReservation);
        }
        //Not making the probe unique for now
//        if(workerIndex.contains(workerIndexReservation)){
//            workerIndexReservation = getIndexHalo(cdf_worker_speed, workerIndex);
//        }
        return workerIndexReservation;
    }

    //Test case in sparrow/src/test/java/edu/berkeley/sparrow/daemon/scheduler/TestPSS.java
    public static double[] getCDFWorkerSpeedHalo(ArrayList<Double> workerSpeedList, double load) throws IOException {
        double[] cdf_worker_speed = new double[workerSpeedList.size()];
        double[] fromHalo = new double[workerSpeedList.size()];

        int j = 0;
        for (double d : workerSpeedList) {
            d = halo(workerSpeedList, d, load);
            fromHalo[j] = d;
            j++;
        }

        double[] rescaled = rescaledValues(fromHalo);

        double cdf = 0;
        int k = 0;
        for (double d : rescaled) {
            cdf = cdf + d;
            cdf_worker_speed[k] = cdf;
            k++;
        }
        //CDF of worker speed + ConfigFunctions based on Qiong's python pss file
        return cdf_worker_speed;
    }

    public static double[] rescaledValues(double[] probabilities) {
        double[] rescaled = new double[probabilities.length];
        double sum = 0;
        for (double w : probabilities) {
            sum = sum + w;
        }
        int i = 0;
        for (double w : probabilities) {
            rescaled[i] = w / sum;
            i++;
        }
        return rescaled;
    }


}
