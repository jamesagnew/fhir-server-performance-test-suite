package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.util.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.ExplanationOfBenefit;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Test02_SearchForEobsByPatient {

	private static final Logger ourLog = LoggerFactory.getLogger(Test02_SearchForEobsByPatient.class);
	private static FhirContext ourCtx;
	private static IGenericClient ourClient;
	private static ArrayList<String> ourPatientIds;

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		String baseUrl = args[0];
		String credentials = args[1];

		int wantPatients = 100;

		ourCtx = FhirContext.forR4();
		ourClient = ourCtx.newRestfulGenericClient(baseUrl);
		ourClient.registerInterceptor(new BasicAuthInterceptor(credentials));

		ourLog.info("Loading some patient IDs...");
		Set<String> patientIds = new HashSet<>();

		Bundle bundle = ourClient
			.search()
			.forResource(Patient.class)
			.returnBundle(Bundle.class)
			.count(1000)
			.execute();
		int page = 1;
		while (true) {
			List<String> nextPatientIds = bundle
				.getEntry()
				.stream()
				.map(Bundle.BundleEntryComponent::getResource)
				.filter(t -> t instanceof Patient)
				.map(t -> t.getIdElement().toUnqualifiedVersionless().getValue())
				.toList();
			patientIds.addAll(nextPatientIds);

			String nextUrl = bundle.getLinkOrCreate("next").getUrl();
			if (patientIds.size() >= wantPatients || isBlank(nextUrl)) {
				break;
			}

			int pathStartIdx = StringUtils.ordinalIndexOf(nextUrl, "/", 3);
			nextUrl = ourClient.getServerBase() + nextUrl.substring(pathStartIdx);

			ourLog.info("Have {} patients, loading page {}", patientIds.size(), ++page);
			bundle = ourClient.loadPage().byUrl(nextUrl).andReturnBundle(Bundle.class).execute();
		}

		ourLog.info("Found {} patients", patientIds.size());
		ourPatientIds = new ArrayList<String>(patientIds);

		int numThreads = 1;
		int numLoads = 10;

		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		List<Future<Long>> futures = new ArrayList<>();
		for (int i = 0; i < numThreads; i++) {
			futures.add(executor.submit(new Loader(numLoads)));
		}

		long totalTime = 0;
		for (var next : futures) {
			totalTime += next.get();
		}

		ourLog.info("Average {}ms / call", totalTime / numLoads);
	}


	private static class Loader implements Callable<Long> {

		private final int myNumLoads;

		public Loader(int theNumLoads) {
			myNumLoads = theNumLoads;
		}

		@Override
		public Long call() throws Exception {
			StopWatch sw = new StopWatch();
			int foundResourceCount = 0;
			for (int i = 0; i < myNumLoads; i++) {
				String patientId = ourPatientIds.get((int) (Math.random() * (double)ourPatientIds.size()));

				Bundle outcome = ourClient
					.search()
					.forResource(ExplanationOfBenefit.class)
					.where(ExplanationOfBenefit.PATIENT.hasId(patientId))
					.returnBundle(Bundle.class)
					.execute();
				foundResourceCount += outcome.getEntry().size();
			}

			ourLog.info("Loaded {} EOBs in {}", foundResourceCount, sw);
			return sw.getMillis();
		}
	}


}
