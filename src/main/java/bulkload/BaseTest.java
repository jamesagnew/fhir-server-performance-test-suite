package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.LenientErrorHandler;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class BaseTest {
	protected final FhirContext myCtx;
	protected final IGenericClient myFhirClient;
	protected final CloseableHttpClient myHttpClient;
	protected final List<String> myBaseUrls;
	protected AtomicLong myBaseUrlCounter = new AtomicLong(0);
	protected AtomicLong myErrorCounter = new AtomicLong(0);

	public BaseTest(List<String> theBaseUrls, String theCredentials) {
		myBaseUrls = theBaseUrls;
		myCtx = FhirContext.forR4Cached();
		myCtx.setParserErrorHandler(new LenientErrorHandler());
		myCtx.getRestfulClientFactory().setSocketTimeout(100000);
		myFhirClient = myCtx.newRestfulGenericClient(theBaseUrls.get(0));
		myFhirClient.registerInterceptor(new BasicAuthInterceptor(theCredentials));

		String encodedCredentials = Base64.encodeBase64String(theCredentials.getBytes(Constants.CHARSET_US_ASCII));

		RequestConfig requestConfig = RequestConfig.custom()
			.setConnectTimeout(60 * 1000)
			.setConnectionRequestTimeout(60 * 1000)
			.setSocketTimeout(60 * 1000)
			.setContentCompressionEnabled(true)
			.build();

		myHttpClient = HttpClientBuilder
			.create()
			.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> request.addHeader("Authorization", "Basic " + encodedCredentials))
			.setMaxConnPerRoute(1000)
			.setMaxConnTotal(1000)
			.setDefaultRequestConfig(requestConfig)
			.build();
	}


	protected String getNextBaseUrl() {
		return myBaseUrls.get((int) (myBaseUrlCounter.incrementAndGet() % myBaseUrls.size()));
	}

}
