package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.codahale.metrics.Histogram;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class Test05_Update extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test05_Update.class);

	public Test05_Update(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "update");
	}

	public void run() throws Exception {
		loadPatients();

		IFunction function = new UpdateTask();
		run(function);
	}

	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test05_Update(baseUrls, credentials).run();
	}

	public static class UpdateTask implements IFunction {
		@Override
		public void run(Histogram theResponseCharCounter, BaseScaleupTest theTest) throws Exception {

			String patientId = theTest.getRandomPatientId();
			String patient = theTest.getPatientString(patientId);

			if (patient.contains("\"gender\":\"male\"")) {
				patient = patient.replace("\"gender\":\"male\"", "\"gender\":\"female\"");
			} else {
				patient = patient.replace("\"gender\":\"female\"", "\"gender\":\"male\"");
			}
			theTest.replacePatient(patientId, patient);

			StringBuilder url = new StringBuilder()
				.append(theTest.getNextBaseUrl())
				.append("/")
				.append(patientId);
			HttpPut request = new HttpPut(url.toString());
			request.setEntity(new StringEntity(patient, CONTENT_TYPE_FHIR_JSON));
			try (var response = theTest.getHttpClient().execute(request)) {
				if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() > 299) {
					ourLog.error("ERROR: Got HTTP status {}", response.getStatusLine().getStatusCode());
					ourLog.error(IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8));
					throw new InternalErrorException("Bad HTTP status");
				}
				int chars = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8).length();
				theResponseCharCounter.update(chars);
			}
		}
	}
}
