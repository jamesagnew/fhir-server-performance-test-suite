package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import com.codahale.metrics.Histogram;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Test03_Create extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test03_Create.class);

	public Test03_Create(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "create");
	}

	public void run() throws Exception {
		loadPatients();

		IFunction function = new CreateTask();

		run(function);
	}


	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test03_Create(baseUrls, credentials).run();
	}

	public static class CreateTask implements IFunction {

		private final String myContent;

		/**
		 * Constructor
		 */
		public CreateTask() {
			Observation obs = new Observation();
			obs.getSubject().setReference("Patient/PATIENTID");
			obs.setEffective(new DateTimeType(DateUtils.addMilliseconds(new Date(), (int) -(10000000 * Math.random()))));
			obs.getCode().addCoding().setSystem("http://foo").setCode("12345");
			obs.setValue(new Quantity(null, 123, "http://bar", "kg", "kg"));
			myContent = FhirContext.forR4Cached().newJsonParser().encodeResourceToString(obs);

		}

		@Override
		public void run(Histogram theResponseCharCounter, BaseScaleupTest theTest) throws Exception {

			String newContent = myContent.replace("Patient/PATIENTID", theTest.getRandomPatientId());

			StringBuilder url = new StringBuilder().append(theTest.getNextBaseUrl()).append("/Observation");
			HttpPost request = new HttpPost(url.toString());
			request.setEntity(new StringEntity(newContent, CONTENT_TYPE_FHIR_JSON));
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
