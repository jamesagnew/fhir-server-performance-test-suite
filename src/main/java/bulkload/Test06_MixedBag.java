package bulkload;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Test06_MixedBag extends BaseScaleupTest {


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
