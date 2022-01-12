package bulkload;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class BaseScaleupTest extends BaseTest {
	private static final Logger ourLog = LoggerFactory.getLogger(BaseScaleupTest.class);

	protected final Logger myCsvLog;
	private ArrayList<String> myPatientIds;

	public BaseScaleupTest(List<String> theBaseUrls, String theCredentials, String theCsvLogName) {
		super(theBaseUrls, theCredentials);
		myCsvLog = LoggerFactory.getLogger(theCsvLogName);
	}

	protected ArrayList<String> getPatientIds() {
		Validate.notNull(myPatientIds);
		return myPatientIds;
	}

	protected void loadPatientIds() {
		int wantPatients = 5000;

		ourLog.info("Loading some patient IDs...");
		Set<String> patientIds = new HashSet<>();

		Bundle bundle = myFhirClient.search().forResource(Patient.class).returnBundle(Bundle.class).count(1000).withAdditionalHeader("Accept-Encoding", "gzip").execute();

		int page = 1;
		while (true) {
			List<String> nextPatientIds = bundle.getEntry().stream().map(Bundle.BundleEntryComponent::getResource).filter(t -> t instanceof Patient).map(t -> t.getIdElement().toUnqualifiedVersionless().getValue()).toList();
			patientIds.addAll(nextPatientIds);

			String nextUrl = bundle.getLinkOrCreate("next").getUrl();
			if (patientIds.size() >= wantPatients || isBlank(nextUrl)) {
				break;
			}

			int pathStartIdx = StringUtils.ordinalIndexOf(nextUrl, "/", 3);
			nextUrl = myFhirClient.getServerBase() + nextUrl.substring(pathStartIdx);

			ourLog.info("Have {} patients, loading page {}", patientIds.size(), ++page);
			bundle = myFhirClient.loadPage().byUrl(nextUrl).andReturnBundle(Bundle.class).execute();
		}

		ourLog.info("Found {} patients", patientIds.size());
		myPatientIds = new ArrayList<>(patientIds);
	}


}
