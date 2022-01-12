package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Quantity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class Test03_Read extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test03_Read.class);

	private static final DecimalFormat ourDecimalFormat = new DecimalFormat("0.0");

	public Test03_Read(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "read");
	}

	public void run() throws Exception {
		loadPatientIds();

		IFunction function = (theResponseCharCounter) -> {

			StringBuilder url = new StringBuilder()
				.append(getNextBaseUrl())
				.append(getRandomPatientId())
				;
			HttpGet request = new HttpGet(url.toString());
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


	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test03_Read(baseUrls, credentials).run();
	}

}
