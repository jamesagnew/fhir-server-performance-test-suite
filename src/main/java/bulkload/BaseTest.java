package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.LenientErrorHandler;
import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import com.codahale.metrics.Histogram;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.UserTokenHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultClientConnectionReuseStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
			.setContentCompressionEnabled(false)
			.setStaleConnectionCheckEnabled(true)
			.build();

		ConnectionReuseStrategy reuseStrategy= DefaultClientConnectionReuseStrategy.INSTANCE;
		myHttpClient = HttpClientBuilder
			.create()
			.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> request.addHeader("Authorization", "Basic " + encodedCredentials))
			.setMaxConnPerRoute(1000)
			.setMaxConnTotal(1000)
			.setConnectionReuseStrategy(new DefaultConnectionReuseStrategy())
			.setConnectionReuseStrategy(reuseStrategy)
			.setDefaultRequestConfig(requestConfig)
			.setConnectionManagerShared(true)
			.setUserTokenHandler(new UserTokenHandler() {
				@Override
				public Object getUserToken(HttpContext context) {
					return null;
				}
			})
			.disableContentCompression()
			.build();
	}


	protected String getNextBaseUrl() {
		return myBaseUrls.get((int) (myBaseUrlCounter.incrementAndGet() % myBaseUrls.size()));
	}

	public static void consumeAndCountResponse(Histogram theResponseCharCounter, CloseableHttpResponse response) throws IOException {
		InputStream content = response.getEntity().getContent();
		int chars = IOUtils.toString(content, StandardCharsets.UTF_8).length();
		response.getEntity().consumeContent();
		theResponseCharCounter.update(chars);
	}
}
