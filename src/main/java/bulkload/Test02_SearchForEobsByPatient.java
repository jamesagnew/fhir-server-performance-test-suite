package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.codahale.metrics.Histogram;
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
		loadPatients();

		IFunction function = new SearchTask();

		run(function);
	}

	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test02_SearchForEobsByPatient(baseUrls, credentials).run();

	}


	public static class SearchTask implements IFunction {
		@Override
		public void run(Histogram theResponseCharCounter, BaseScaleupTest theTest) throws Exception {
			String patientId = theTest.getRandomPatientId();
			String baseUrl = theTest.getNextBaseUrl();

			StringBuilder url = new StringBuilder().append(baseUrl).append("/ExplanationOfBenefit?patient=").append(patientId);
			HttpGet request = new HttpGet(url.toString());
			try (var response = theTest.getHttpClient().execute(request)) {
				if (response.getStatusLine().getStatusCode() != 200) {
					ourLog.error("ERROR: Got HTTP status {}", response.getStatusLine().getStatusCode());
					throw new InternalErrorException("Bad HTTP status");
				}

				int chars = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8).length();
				theResponseCharCounter.update(chars);
			}
		}
	}
}
