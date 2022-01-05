package bulkload;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.jpa.search.reindex.BlockPolicy;
import ca.uhn.fhir.rest.client.apache.ApacheRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.util.StopWatch;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class SyntheaPerfTest {
	private static final Logger ourLog = LoggerFactory.getLogger(SyntheaPerfTest.class);
	private static final FhirContext ourCtx;
	private static List<IGenericClient> ourClients = new ArrayList<>();
	private static List<AtomicInteger> ourClientInvocationCounts = new ArrayList<>();
	private static List<AtomicLong> ourClientInvocationTimes = new ArrayList<>();
	private static int ourMaxThreads;
	private static int ourOffset;

	static {

		ourCtx = FhirContext.forR4();
		ApacheRestfulClientFactory clientFactory = new ApacheRestfulClientFactory(ourCtx);
		clientFactory.setPoolMaxPerRoute(100);
		clientFactory.setPoolMaxTotal(100);
		ourCtx.setRestfulClientFactory(clientFactory);
	}

	private static class Uploader {

		private final ThreadPoolTaskExecutor myExecutor;
		private final StopWatch mySw;
		private final AtomicInteger myFilesCounter = new AtomicInteger(0);
		private final AtomicInteger myErrorsCounter = new AtomicInteger(0);
		private final AtomicLong myResourcesCounter = new AtomicLong(0);
		private final AtomicInteger myClientIndex = new AtomicInteger(0);
		private final int myPathsCount;

		public Uploader(List<Path> thePaths) throws ExecutionException, InterruptedException {
			Validate.isTrue(thePaths.size() > 0);

			myExecutor = new ThreadPoolTaskExecutor();
			myExecutor.setCorePoolSize(ourMaxThreads);
			myExecutor.setMaxPoolSize(ourMaxThreads);
			myExecutor.setQueueCapacity(10);
			myExecutor.setAllowCoreThreadTimeOut(true);
			myExecutor.setThreadNamePrefix("Uploader-");
			myExecutor.setRejectedExecutionHandler(new BlockPolicy());
			myExecutor.initialize();

			mySw = new StopWatch();
			List<Future<?>> futures = new ArrayList<>();
			myPathsCount = thePaths.size();
			for (Path next : thePaths) {
				futures.add(myExecutor.submit(new MyTask(next)));
			}

			for (Future<?> next : futures) {
				next.get();
			}

			ourLog.info("Finished uploading {} files with {} resources in {} - {} files/sec - {} res/sec",
				myFilesCounter.get() + ourOffset,
				myResourcesCounter.get(),
				mySw,
				mySw.formatThroughput(myFilesCounter.get(), TimeUnit.SECONDS),
				mySw.formatThroughput(myResourcesCounter.get(), TimeUnit.SECONDS));
		}

		private class MyTask implements Runnable {

			private final Path myPath;

			public MyTask(Path thePath) {
				myPath = thePath;
			}

			@Override
			public void run() {
				try {
					String bundle;
					try (FileReader reader = new FileReader(myPath.toFile())) {
						bundle = IOUtils.toString(reader);
					} catch (IOException e) {
						throw new InternalErrorException(e);
					}
					if (isBlank(bundle)) {
						return;
					}

					try {
						int clientIndex = myClientIndex.incrementAndGet() % ourClients.size();
						IGenericClient client = ourClients.get(clientIndex);

						long start = System.currentTimeMillis();
						client
							.transaction()
							.withBundle(bundle)
							.execute();
						long latency = System.currentTimeMillis() - start;

						ourClientInvocationCounts.get(clientIndex).incrementAndGet();
						ourClientInvocationTimes.get(clientIndex).addAndGet(latency);

						// Subtract by 1 because of the Bundle resource
						int resourceCount = StringUtils.countMatches(bundle, "resourceType") - 1;
						myResourcesCounter.addAndGet(resourceCount);

					} catch (BaseServerResponseException e) {
						ourLog.error("Failure: {}", e.toString());
						myErrorsCounter.incrementAndGet();
					}

					int fileCount = myFilesCounter.incrementAndGet();
					if (fileCount % 10 == 0) {
						ourLog.info("Have uploaded {}/{} files with {} resources in {} - {} files/sec - {} res/sec - ETA {} - {} errors",
							myFilesCounter.get() + ourOffset,
							myPathsCount + ourOffset,
							myResourcesCounter.get(),
							mySw,
							mySw.formatThroughput(myFilesCounter.get(), TimeUnit.SECONDS),
							mySw.formatThroughput(myResourcesCounter.get(), TimeUnit.SECONDS),
							mySw.getEstimatedTimeRemaining(myFilesCounter.get(), myPathsCount),
							myErrorsCounter.get());
						ourLog.info("NEXT,{},{},{},{},{},{}",
							mySw.getMillis(),
							myFilesCounter.get() + ourOffset,
							myResourcesCounter.get(),
							mySw.formatThroughput(myFilesCounter.get(), TimeUnit.SECONDS),
							mySw.formatThroughput(myResourcesCounter.get(), TimeUnit.SECONDS),
							myErrorsCounter.get());

						StringBuilder timings = new StringBuilder();
						timings.append("Timings:");
						for (int i = 0; i < ourClientInvocationCounts.size(); i++) {
							timings.append("\n * ");
							timings.append(ourClients.get(i).getServerBase());
							timings.append(" - Avg ");
							long avg = ourClientInvocationTimes.get(i).get() / (long) ourClientInvocationCounts.get(i).get();
							timings.append(avg);
							timings.append("ms");
						}
						ourLog.info(timings.toString());
					}
				} catch (Throwable t) {
					myErrorsCounter.incrementAndGet();
					ourLog.error("Failure during task", t);
				}
			}
		}


	}

	public static void main(String[] args) throws Exception {

		String directory = args[0];
		String baseUrls = args[1];
		String credentials = args[2];
		String threads = args[3];
		String uploadMetadata = args[4];
		if (args.length >= 6) {
			String offset = args[5];
			ourOffset = Integer.parseInt(offset);
		}

		ourMaxThreads = Integer.parseInt(threads);

		ourLog.info("Searching for Synthea files in directory: {}", directory);
		List<Path> files = Files
			.list(FileSystems.getDefault().getPath(directory))
			.filter(t -> t.toString().endsWith(".json"))
			.sorted(Comparator.comparing(Path::toString))
			.collect(Collectors.toList());

		if (files.size() < 10) {
			throw new Exception(files.size() + " .json files found in " + directory);
		}

		if (ourOffset > 0) {
			ourLog.info("Starting at offset {}", ourOffset);
			files = files.subList(ourOffset, files.size());
		}


		ourCtx.getRestfulClientFactory().setConnectionRequestTimeout(1000000);
		ourCtx.getRestfulClientFactory().setConnectTimeout(1000000);
		ourCtx.getRestfulClientFactory().setSocketTimeout(1000000);

		String[] baseUrlSplit = baseUrls.split(",");
		for (String next : baseUrlSplit) {
			ourLog.info("Uploading to FHIR server at base URL: {}", next);
			IGenericClient client = ourCtx.newRestfulGenericClient(next);
			client.registerInterceptor(new BasicAuthInterceptor(credentials));
			client.capabilities().ofType(CapabilityStatement.class).execute();
			ourClients.add(client);
			ourClientInvocationCounts.add(new AtomicInteger());
			ourClientInvocationTimes.add(new AtomicLong());
		}

		if (uploadMetadata.equals("true")) {
			ourLog.info("Loading metadata files...");
			List<Path> meta = files.stream().filter(t -> t.toString().contains("hospital") || t.toString().contains("practitioner")).collect(Collectors.toList());
			new Uploader(meta);
		}

		ourLog.info("Loading non metadata files...");
		List<Path> nonMeta = files.stream().filter(t -> !t.toString().contains("hospital") && !t.toString().contains("practitioner")).collect(Collectors.toList());

//		ourLog.info("Preloading a few files single-threaded in order to seed tags...");
//		new Uploader(Collections.singletonList(nonMeta.remove(0)));
//		new Uploader(Collections.singletonList(nonMeta.remove(0)));
//		new Uploader(Collections.singletonList(nonMeta.remove(0)));
//		new Uploader(Collections.singletonList(nonMeta.remove(0)));

		ourLog.info("Starting real load with {} threads...", ourMaxThreads);
		new Uploader(nonMeta);
	}


}
