package nz.net.nzrs.hive.udf;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class ConfickerDistanceTests {

	/**
	 * Helper method.
	 * @return list of strings
	 */
	private List<String> getStrings(){
		List<String> strings = new ArrayList<String>();
		strings.add("abel");
		strings.add("bell");
		strings.add("bill");
		return strings;
	}
	
	/**
	 * Helper method.
	 * @return list of strings
	 */
	private List<String> getStrings2(){
		List<String> strings = new ArrayList<String>();
		strings.add("abel");
		strings.add("tasman");
		strings.add("bhattacharyya");
		strings.add("taranaki");
		strings.add("mecca");
		strings.add("medina");
		return strings;
	}
	
	/**
	 * Helper method.
	 * @return a ConfickerDistance object.
	 */
	private ConfickerDistance makeUDF(){
		try {
			return new ConfickerDistance();
		} catch (HiveException e) {
			throw new Error();
		}
	}
	
	@Test
	public void testNgramCounts001(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		String str = "abel";
		
		// check correct probabilities
		Map<String, Float> fcount = cd.computeNgramProbabilities(str, true);
		assertTrue(fcount.get("ab") == 1f/3);
		assertTrue(fcount.get("be") == 1f/3);
		assertTrue(fcount.get("el") == 1f/3);
	}
	
	@Test
	public void testNgramCounts002(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		String str = "bellell";
		
		// check correct probabilities
		Map<String, Float> fcount = cd.computeNgramProbabilities(str, true);
		assertTrue(fcount.get("be") == 1f/6);
		assertTrue(fcount.get("el") == 2f/6);
		assertTrue(fcount.get("ll") == 2f/6);
		assertTrue(fcount.get("le") == 1f/6);
	}
	
	@Test
	public void testNgramCounts003(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		String str = "";
		
		// check correct probabilities
		Map<String, Float> fcount = cd.computeNgramProbabilities("", true);
		assertTrue(fcount.isEmpty());
	}
	
	@Test
	public void testNgramCounts004(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		List<String> strings = new ArrayList<String>();
		strings.add("ab"); strings.add("ab"); strings.add("bc"); strings.add("bc");
		strings.add("de"); strings.add("cd"); strings.add("cd"); strings.add("ba");
		
		// check correct probabilities
		Map<String, Float> fcount = cd.computeNgramProbabilities(strings, true);
		assertTrue(fcount.get("ab") == 2f/8);
		assertTrue(fcount.get("bc") == 2f/8);
		assertTrue(fcount.get("de") == 1f/8);
		assertTrue(fcount.get("cd") == 2f/8);
		assertTrue(fcount.get("ba") == 1f/8);
	}

	@Test
	public void testNormalise001(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		Map<String, Float> map = new HashMap<String, Float>();
		List<String> strings = getStrings2();
		
		// make up some numbers to normalise
		int numObservations = 0;
		for (String str : strings){
			int randint = (int)(20 * Math.random());
			numObservations += randint;
			map.put(str, new Float(randint));
		}
		
		// normalise, check it adds up to 1
		cd.normalise(map, numObservations);
		float sum = 0f;
		for (Float fl : map.values()){
			sum += fl;
		}
		assertTrue("Normalised map should sum to 1f but it sums to: " + sum, sum==1f);
	}
	
	@Test
	public void testNormalise002(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		String s1 = "john";
		List<String> strings = new ArrayList<String>();
		strings.add(s1);
		
		// make map, normalise, check it adds up to 1
		Map<String, Float> map = cd.computeNgramProbabilities(strings, true);
		float sum = 0f;
		for (Float fl : map.values()){
			sum += fl;
		}
		assertTrue("Normalised map should sum to 1f but it sums to: " + sum, sum==1f);
	}
	
	@Test
	public void testSmoothing001(){
		
		// set up
		ConfickerDistance cd = makeUDF();
		List<String> strings = getStrings();
		
		// Laplace smoothing on strings, calculate probability distribution
		cd.smooth(strings);
		Map<String, Float> probs = cd.computeNgramProbabilities(strings, true);
		
		// check each ASCII char is defined in the probability distribution
		for (int c = 1; c < 128; c++){
			char char1 = (char)c;
			for (int d = 1; d < 128; d++){
				char char2 = (char)d;
				char[] chars = {char1, char2};
				String ngram = new String(chars);
				if (!probs.containsKey(ngram)) fail("Probs should contain " + ngram);
			}
		}
	}
	
}
