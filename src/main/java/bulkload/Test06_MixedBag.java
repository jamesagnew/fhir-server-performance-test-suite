package bulkload;

import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Test06_MixedBag extends BaseScaleupTest {


	private static final Logger ourLog = LoggerFactory.getLogger(Test06_MixedBag.class);

	public Test06_MixedBag(List<String> theBaseUrls, String theCredentials) {
		super(theBaseUrls, theCredentials, "mixedbag");
	}

	private void run() throws ExecutionException, InterruptedException {
		loadPatients();

		run(
			new Test02_SearchForEobsByPatient.SearchTask(),
			new Test03_Create.CreateTask(),
			new Test04_Read.ReadTask(),
			new Test05_Update.UpdateTask()
		);
	}

	public static void main(String[] args) throws Exception {

		List<String> baseUrls = Arrays.asList(args[0].split(","));
		String credentials = args[1];

		new Test06_MixedBag(baseUrls, credentials).run();
	}

}
