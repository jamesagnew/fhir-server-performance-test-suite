package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Test02_SearchForEobsByPatient extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test02_SearchForEobsByPatient.class);

	public Test02_SearchForEobsByPatient(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "search");
	}

	public void run() throws Exception {
		loadPatientIds();

		IFunction function = (theResponseCharCounter) -> {
			String patientId = getRandomPatientId();
			String baseUrl = getNextBaseUrl();

			StringBuilder url = new StringBuilder()
				.append(baseUrl)
				.append("/ExplanationOfBenefit?patient=")
				.append(patientId);
			HttpGet request = new HttpGet(url.toString());
			try (var response = myHttpClient.execute(request)) {
				if (response.getStatusLine().getStatusCode() != 200) {
					ourLog.error("ERROR: Got HTTP status {}", response.getStatusLine().getStatusCode());
					throw new InternalErrorException("Bad HTTP status");
				}

				int chars = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8).length();
				theResponseCharCounter.update(chars);
			}
		};

		run(function);
	}

	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test02_SearchForEobsByPatient(baseUrls, credentials).run();

	}


}
