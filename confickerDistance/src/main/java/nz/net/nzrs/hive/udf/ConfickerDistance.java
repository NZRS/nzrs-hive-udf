package nz.net.nzrs.hive.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;

public class ConfickerDistance extends UDF{

	private Map<String, Float> confickerDistribution;
	private List<String> tlds;
	
	public ConfickerDistance() throws HiveException{
		String dir = "confickerDistance/src/resources/";
		dir.replace('/', File.separatorChar);
		
		// read conficker
		String fname = dir + "conficker.txt";
		try{
			File file = new File(fname);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			List<String> names = new ArrayList<String>();		
			String line = null;
			while ((line=br.readLine()) != null){
				names.add(line);
			}
			smooth(names);
			br.close();
			confickerDistribution = computeNgramProbabilities(names, true);
		}
		catch(FileNotFoundException fnfe){
			throw new HiveException("Could not find " + fname, fnfe);
		}
		catch(IOException ie){
			throw new HiveException("Error parsing " + fname, ie);
		}
		
		// read tlds
		fname = dir + "tlds.txt";
		try{
			File file = new File(fname);
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			tlds = new ArrayList<String>();
			String line = null;
			while ((line=br.readLine()) != null){
				tlds.add(line);
			}
			br.close();
		}
		catch(FileNotFoundException fnfe){
			throw new HiveException("Could not find " + fname, fnfe);
		}
		catch(IOException ie){
			throw new HiveException("Error parsing " + fname, ie);
		}
		
	}
	
	protected Map<String, Float> computeNgramProbabilities(List<String> strings, boolean normalise){
		
		// Get the number of observations per ngram
		Map<String, Float> fcount = new HashMap<String, Float>();
		int numObservations = 0;
		for (String str : strings){
			for (int i = 0; i < str.length()-1; i++){
				String ngram = str.substring(i, i+2);
				if (fcount.containsKey(ngram)){
					fcount.put(ngram, fcount.get(ngram) + 1);
				}
				else{
					fcount.put(ngram, 1f);
				}
				numObservations++;
			}
		}
	
		// Normalise the observations
		if (normalise) normalise(fcount, numObservations);

		return fcount;
	}
	
	protected Map<String, Float> computeNgramProbabilities(String string, boolean normalise){
		
		// Get the number of observations per ngram
		Map<String, Float> fcount = new HashMap<String, Float>();
		int numObservations = 0;
		for (int i = 0; i < string.length()-1; i++){
			String ngram = string.substring(i, i+2);
			if (fcount.containsKey(ngram)){
				fcount.put(ngram, fcount.get(ngram) + 1);
			}
			else{
				fcount.put(ngram, 1f);
			}
			numObservations++;
		}
		
		if (normalise) normalise(fcount, numObservations);
		
		return fcount;
	}

	protected void normalise(Map<String, Float> map, int numObservations){
		for (Map.Entry<String, Float> entry : map.entrySet()){
			String ngram = entry.getKey();
			Float count = entry.getValue();
			count = count / numObservations;
			map.put(ngram, count);
		}
	}
	
	protected void smooth(List<String> ngrams){
		for (int c=1 ; c < 128; c++){
			char char1 = (char)c;
			for (int d=1 ; d < 128; d++){
				char char2 = (char)d;
				char[] chars = {char1, char2};
				String bigram = new String(chars);
				ngrams.add(bigram);
			}
		}
	}

	/**
	 * Computes the Bhattacharyya distance between the bi-gram probability distributions of a string to conficker.
	 * @param dist1 : bi-gram probability distribution of a string.
	 * @return the distance between the string's probability distribution and conficker's.
	 */
	protected double distanceToConficker(Map<String, Float> ngrams){
		float distance = 0f;
		for (String ngram : ngrams.keySet()){
			float p_dist1 = ngrams.get(ngram);
			float p_conficker = confickerDistribution.get(ngram);
			distance += Math.sqrt(p_dist1 * p_conficker);
		}
		return -1 * Math.log(distance);
	}
	
	public boolean evaluate(final Text input, final float threshold){
		String string = input.toString();
		Map<String, Float> ngrams = computeNgramProbabilities(string, true);
		return distanceToConficker(ngrams) < threshold;
	}
	
	public boolean evaluate(final Text input, final int threshold){
		return evaluate(input, (float)threshold);
	}
	
	public boolean evaluate(final Text input){
		return evaluate(input, 3f);
	}
	
}
