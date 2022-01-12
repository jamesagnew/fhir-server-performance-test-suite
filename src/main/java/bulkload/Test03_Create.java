package bulkload;

import ca.uhn.fhir.util.FileUtil;
import ca.uhn.fhir.util.StopWatch;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.client.methods.HttpGet;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Test03_Create extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test03_Create.class);

	private static final DecimalFormat ourDecimalFormat = new DecimalFormat("0.0");

	public Test03_Create(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "create");
	}

	public void run() throws Exception {
		IFunction function = (theResponseCharCounter) -> {

			Patient pt = new Patient();
			pt.addName()
				.setFamily("Simpson")
				.addGiven("Homer")
				.addGiven("J");
			pt.addIdentifier()
				.setSystem("http://foo")
				.setValue(UUID.randomUUID().toString());
			pt.setBirthDate(DateUtils.addDays(new Date(), -((int) (Math.random() * 3000))));
			String content = myCtx.newJsonParser().encodeResourceToString(pt);

			String patientId = getPatientIds().get((int) (Math.random() * (double) getPatientIds().size()));
			String baseUrl = getNextBaseUrl();

			StringBuilder url = new StringBuilder()
				.append(baseUrl)
				.append("/ExplanationOfBenefit?patient=")
				.append(patientId)
//				.append("&_summary=count")
				;
			HttpGet request = new HttpGet(url.toString());
			try (var response = myHttpClient.execute(request)) {
				Validate.isTrue(response.getStatusLine().getStatusCode() == 200, "Received bad status code: %s", response.getStatusLine().getStatusCode());
				int chars = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8).length();
				theResponseCharCounter.update(chars);
			}
		};

		run(function);
	}

	protected void run(IFunction theFunction) throws ExecutionException, InterruptedException {
		int pass = 0;
		int numThreads;
		int numLoads = 3;

		for (numThreads = 1; numThreads < 100; numThreads++) {
			for (int i = 0; i < 3; i++) {
				pass++;
				performPass(pass, numThreads, numLoads, theFunction);
			}
		}
	}

	private void performPass(int pass, int numThreads, int numLoads, IFunction theFunction) throws InterruptedException, ExecutionException {
		MetricRegistry metricRegistry = new MetricRegistry();
		Timer latencyTimer = metricRegistry.timer("latencyTimer-" + numThreads);
		Histogram responseCharCount = metricRegistry.histogram("response-charcount-" + numThreads);

		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		List<Future<Long>> futures = new ArrayList<>();

		StopWatch sw = new StopWatch();
		for (int i = 0; i < numThreads; i++) {
			futures.add(executor.submit(new Loader(numLoads, latencyTimer, responseCharCount, theFunction)));
		}

		for (var next : futures) {
			next.get();
		}

		double mean = latencyTimer.getSnapshot().getMean();
		double pct75 = latencyTimer.getSnapshot().get75thPercentile();
		double pct98 = latencyTimer.getSnapshot().get98thPercentile();
		double pct99 = latencyTimer.getSnapshot().get99thPercentile();
		int totalSearches = numThreads * numLoads;
		ourLog.info("Pass {} Finished {} searches across {} threads - Mean {}ms - 75th pct {}ms - 98th pct {}ms - 99th pct {}ms - Average response {} - Max response {} - Overall throughput {} req/sec", pass, totalSearches, numThreads, formatNanos(mean), formatNanos(pct75), formatNanos(pct98), formatNanos(pct99), FileUtil.formatFileSize((long) responseCharCount.getSnapshot().getMean()), FileUtil.formatFileSize(responseCharCount.getSnapshot().getMax()), sw.formatThroughput(totalSearches, TimeUnit.SECONDS));
		myCsvLog.info(",NEXT,{},{},{},{},{},{},{},{},{},{}", pass, totalSearches, numThreads, formatNanos(mean), formatNanos(pct75), formatNanos(pct98), formatNanos(pct99), ourDecimalFormat.format((long) responseCharCount.getSnapshot().getMean() / 1024), ourDecimalFormat.format(responseCharCount.getSnapshot().getMax() / 1024), sw.formatThroughput(totalSearches, TimeUnit.SECONDS));

		executor.shutdown();
	}

	private interface IFunction {

		void run(Histogram theResponseCharCounter) throws Exception;

	}

	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test03_Create(baseUrls, credentials).run();

	}

	private class Loader implements Callable<Long> {

		private final int myNumLoads;
		private final Timer myTimer;
		private final Histogram myResponseCharCounter;
		private final IFunction myFunction;

		public Loader(int theNumLoads, Timer theTimer, Histogram theResponseCharCounter, IFunction theFunction) {
			myNumLoads = theNumLoads;
			myTimer = theTimer;
			myResponseCharCounter = theResponseCharCounter;
			myFunction = theFunction;
		}

		@Override
		public Long call() throws Exception {
			for (int i = 0; i < myNumLoads; i++) {
				StopWatch sw = new StopWatch();
				myFunction.run(myResponseCharCounter);
				long millis = sw.getMillis();
				myTimer.update(millis, TimeUnit.MILLISECONDS);
			}

			return null;
		}
	}
}
