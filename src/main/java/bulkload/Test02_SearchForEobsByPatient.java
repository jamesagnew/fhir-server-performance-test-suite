package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test02_SearchForEobsByPatient {

	private static final Logger ourLog = LoggerFactory.getLogger(Test02_SearchForEobsByPatient.class);
	private static FhirContext ourCtx;
	private static IGenericClient ourClient;

	public static void main(String[] args) throws IOException {

		String baseUrl = args[0];
		String credentials = args[1];

		ourCtx = FhirContext.forR4();
		ourClient = ourCtx.newRestfulGenericClient(baseUrl);
		ourClient.registerInterceptor(new BasicAuthInterceptor(credentials));

		ourLog.info("Loading some patient IDs...");
		Set<String> patientIds = new HashSet<>();

		Bundle bundle = ourClient
			.search()
			.forResource(Patient.class)
			.returnBundle(Bundle.class)
			.execute();
		int page = 1;
		while (true) {
			List<String> nextPatientIds = bundle
				.getEntry()
				.stream()
				.map(Bundle.BundleEntryComponent::getResource)
				.filter(t -> t instanceof Patient)
				.map(t -> t.getIdElement().toUnqualifiedVersionless().getValue())
				.toList();
			patientIds.addAll(nextPatientIds);

			if (nextPatientIds.size() > 10000 || bundle.getLink("next") == null) {
				break;
			}

			ourLog.info("Have {} patients, loading page {}", patientIds.size(), ++page);
			bundle = ourClient.loadPage().next(bundle).execute();
		}

		ourLog.info("Found {} patients", patientIds.size());

	}

}
