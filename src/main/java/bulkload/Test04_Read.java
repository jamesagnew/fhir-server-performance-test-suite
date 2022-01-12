package bulkload;

import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

public class Test04_Read extends BaseScaleupTest {

	private static final Logger ourLog = LoggerFactory.getLogger(Test04_Read.class);

	public Test04_Read(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "read");
	}

	public void run() throws Exception {
		loadPatientIds();

		IFunction function = (theResponseCharCounter) -> {

			StringBuilder url = new StringBuilder()
				.append(getNextBaseUrl())
				.append("/")
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

		new Test04_Read(baseUrls, credentials).run();
	}

}
