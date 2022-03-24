package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.util.FileUtil;
import ca.uhn.fhir.util.StopWatch;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class BaseScaleupTest extends BaseTest {
	public static final ContentType CONTENT_TYPE_FHIR_JSON = ContentType.parse("application/fhir+json");
	private static final Logger ourLog = LoggerFactory.getLogger(BaseScaleupTest.class);
	private static final DecimalFormat ourDecimalFormat = new DecimalFormat("0.0");
	protected final Logger myCsvLog;
	private final String myCsvLogName;
	protected AtomicLong myFunctionCounter = new AtomicLong(0);
	private ArrayList<String> myPatientIds;
	private Map<String, String> myPatients;
	private final AtomicLong myNextPatientIndex = new AtomicLong(0);

	public BaseScaleupTest(List<String> theBaseUrls, String theCredentials, String theCsvLogName) {
		super(theBaseUrls, theCredentials);
		myCsvLog = LoggerFactory.getLogger(theCsvLogName);
		myCsvLogName = theCsvLogName;
	}

	protected String getRandomPatientId() {
		Validate.notNull(myPatientIds);
		return myPatientIds.get((int) (myNextPatientIndex.incrementAndGet() % myPatientIds.size()));
	}

	public String getPatientString(String thePatientId) {
		String retVal = myPatients.get(thePatientId);
		Validate.notNull(retVal);
		return retVal;
	}

	protected void loadPatients() {
		int wantPatients = 5000;

		ourLog.info("Loading some patient IDs...");
		Set<String> patientIds = new HashSet<>();
		Map<String, String> patients = new HashMap<>();

		Bundle bundle = myFhirClient
			.search()
			.forResource(Patient.class)
			.returnBundle(Bundle.class)
			.count(1000)
			.withAdditionalHeader("Accept-Encoding", "gzip")
			.execute();

		int page = 1;
		while (true) {
			bundle
				.getEntry()
				.stream()
				.map(Bundle.BundleEntryComponent::getResource)
				.filter(t -> t instanceof Patient)
				.forEach(p -> {
					String id = p.getIdElement().toUnqualifiedVersionless().getValue();
					patientIds.add(id);
					patients.put(id, myCtx.newJsonParser().encodeResourceToString(p));
				});

			String nextUrl = bundle.getLinkOrCreate("next").getUrl();
			if (patientIds.size() >= wantPatients || isBlank(nextUrl)) {
				break;
			}

			if (!nextUrl.contains("hapi.fhir.org")) {
				int pathStartIdx = StringUtils.ordinalIndexOf(nextUrl, "/", 3);
				nextUrl = myFhirClient.getServerBase() + nextUrl.substring(pathStartIdx);
			}

			ourLog.info("Have {} patients, loading page {}", patientIds.size(), ++page);
			bundle = myFhirClient.loadPage().byUrl(nextUrl).andReturnBundle(Bundle.class).execute();
		}

		ourLog.info("Found {} patients", patientIds.size());
		myPatientIds = new ArrayList<>(patientIds);
		myPatients = Collections.synchronizedMap(new HashMap<>(patients));

	}

	protected void run(IFunction... theFunctions) throws ExecutionException, InterruptedException {
		myCsvLog.info("Timestamp,NEXT,Pass,Searches Performed,Concurrent Users,Min (ms),Mean (ms),75th Percentile (ms),98th Percentile (ms),99th Percentile (ms),Max (ms),Average Response (kb),Max Response (kb),Throughput / Sec,Errors");

		int pass = 0;
		int numThreads;
		int numLoads = 10;

		for (numThreads = 1; numThreads < 200; numThreads++) {
			for (int i = 0; i < 3; i++) {
				pass++;
				performPass(pass, numThreads, numLoads, theFunctions);
			}
		}
	}

	private void performPass(int pass, int numThreads, int numLoads, IFunction[] theFunctions) throws InterruptedException, ExecutionException {
		MetricRegistry metricRegistry = new MetricRegistry();
		Timer latencyTimer = metricRegistry.timer("latencyTimer-" + numThreads);
		Histogram responseCharCount = metricRegistry.histogram("response-charcount-" + numThreads);

		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		List<Future<Long>> futures = new ArrayList<>();

		StopWatch sw = new StopWatch();
		for (int i = 0; i < numThreads; i++) {
			futures.add(executor.submit(new Loader(numLoads, latencyTimer, responseCharCount, theFunctions)));
		}

		for (var next : futures) {
			next.get();
		}

		double min = latencyTimer.getSnapshot().getMin();
		double mean = latencyTimer.getSnapshot().getMean();
		double pct75 = latencyTimer.getSnapshot().get75thPercentile();
		double pct98 = latencyTimer.getSnapshot().get98thPercentile();
		double pct99 = latencyTimer.getSnapshot().get99thPercentile();
		double max = latencyTimer.getSnapshot().getMax();
		int totalSearches = numThreads * numLoads;
		ourLog.info("Pass {} Finished {} {} across {} threads - Min {}ms - Mean {}ms - 75th pct {}ms - 98th pct {}ms - 99th pct {}ms - Average response {} - Max response {} - Overall throughput {} req/sec - {} errors", pass, totalSearches, myCsvLogName, numThreads, formatNanos(min), formatNanos(mean), formatNanos(pct75), formatNanos(pct98), formatNanos(pct99), FileUtil.formatFileSize((long) responseCharCount.getSnapshot().getMean()), FileUtil.formatFileSize(responseCharCount.getSnapshot().getMax()), sw.formatThroughput(totalSearches, TimeUnit.SECONDS), myErrorCounter.get());
		myCsvLog.info(",NEXT,{},{},{},{},{},{},{},{},{},{},{},{},{}", pass, totalSearches, numThreads, formatNanos(min), formatNanos(mean), formatNanos(pct75), formatNanos(pct98), formatNanos(pct99), formatNanos(max), ourDecimalFormat.format((long) responseCharCount.getSnapshot().getMean() / 1024), ourDecimalFormat.format(responseCharCount.getSnapshot().getMax() / 1024), sw.formatThroughput(totalSearches, TimeUnit.SECONDS), myErrorCounter.get());

		executor.shutdown();
	}

	public CloseableHttpClient getHttpClient() {
		return myHttpClient;
	}

	public void replacePatient(String thePatientId, String thePatient) {
		myPatients.put(thePatientId, thePatient);
	}

	protected interface IFunction {

		void run(Histogram theResponseCharCounter, BaseScaleupTest theTest) throws Exception;

	}

	protected static String formatNanos(double mean) {
		return ourDecimalFormat.format(mean / 1000000.0);
	}


	private class Loader implements Callable<Long> {

		private final int myNumLoads;
		private final Timer myTimer;
		private final Histogram myResponseCharCounter;
		private final IFunction[] myFunctions;

		public Loader(int theNumLoads, Timer theTimer, Histogram theResponseCharCounter, IFunction[] theFunctions) {
			myNumLoads = theNumLoads;
			myTimer = theTimer;
			myResponseCharCounter = theResponseCharCounter;
			myFunctions = theFunctions;
		}

		@Override
		public Long call() throws Exception {
			for (int i = 0; i < myNumLoads; i++) {
				StopWatch sw = new StopWatch();
				try {
					IFunction function = myFunctions[(int) (myFunctionCounter.incrementAndGet() % myFunctions.length)];
					function.run(myResponseCharCounter, BaseScaleupTest.this);
				} catch (InternalErrorException e) {
					myErrorCounter.incrementAndGet();
				}
				long millis = sw.getMillis();
				myTimer.update(millis, TimeUnit.MILLISECONDS);
			}

			return null;
		}
	}
}
