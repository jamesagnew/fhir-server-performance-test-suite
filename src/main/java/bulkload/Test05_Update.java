package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Test05_Update extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test05_Update.class);

	private List<String> myPatientIds;
	private Map<String, String> myPatients;
	private AtomicLong myNextPatientIndex = new AtomicLong(0);

	public Test05_Update(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "update");
	}

	public void run() throws Exception {
		loadPatients();

		IFunction function = (theResponseCharCounter) -> {

			String patientId = myPatientIds.get((int) (myNextPatientIndex.incrementAndGet() % myPatientIds.size()));
			String patient = myPatients.get(patientId);

			if (patient.contains("\"gender\":\"male\"")) {
				patient = patient.replace("\"gender\":\"male\"", "\"gender\":\"female\"");
			} else {
				patient = patient.replace("\"gender\":\"female\"", "\"gender\":\"male\"");
			}
			myPatients.put(patientId, patient);

			StringBuilder url = new StringBuilder()
				.append(getNextBaseUrl())
				.append("/")
				.append(patientId);
			HttpPut request = new HttpPut(url.toString());
			request.setEntity(new StringEntity(patient, CONTENT_TYPE_FHIR_JSON));
			try (var response = myHttpClient.execute(request)) {
				if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() > 299) {
					ourLog.error("ERROR: Got HTTP status {}", response.getStatusLine().getStatusCode());
					ourLog.error(IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8));
					throw new InternalErrorException("Bad HTTP status");
				}
				int chars = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8).length();
				theResponseCharCounter.update(chars);
			}
		};

		run(function);
	}

	private void loadPatients() {
		int wantPatients = 5000;

		ourLog.info("Loading some patient IDs...");
		List<String> patientIds = new ArrayList<>();
		Map<String, String> patients = new HashMap<>();

		Bundle bundle = myFhirClient.search().forResource(Patient.class).returnBundle(Bundle.class).count(1000).withAdditionalHeader("Accept-Encoding", "gzip").execute();

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

			int pathStartIdx = StringUtils.ordinalIndexOf(nextUrl, "/", 3);
			nextUrl = myFhirClient.getServerBase() + nextUrl.substring(pathStartIdx);

			ourLog.info("Have {} patients, loading page {}", patientIds.size(), ++page);
			bundle = myFhirClient.loadPage().byUrl(nextUrl).andReturnBundle(Bundle.class).execute();
		}

		ourLog.info("Found {} patients", patientIds.size());
		myPatientIds = new ArrayList<>(patientIds);
		myPatients = Collections.synchronizedMap(new HashMap<>(patients));
	}


	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test05_Update(baseUrls, credentials).run();
	}

}
