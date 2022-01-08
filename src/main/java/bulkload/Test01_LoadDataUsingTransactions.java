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

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class Test01_LoadDataUsingTransactions {
	private static final Logger ourLog = LoggerFactory.getLogger(Test01_LoadDataUsingTransactions.class);
	private static final FhirContext ourCtx;
	private static List<IGenericClient> ourClients = new ArrayList<>();
	private static List<ThreadTiming> ourClientInvocationCounts = new ArrayList<>();
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

			ourLog.info("Going to submit {} tasks", thePaths.size());

			for (int i = 0, thePathsSize = thePaths.size(); i < thePathsSize; i++) {
				if (i % 1000 == 0) {
					ourLog.info("Have submitted {} tasks", i);
				}
				Path next = thePaths.get(i);
				futures.add(myExecutor.submit(new MyTask(next)));
			}

			ourLog.info("Have submitted {} tasks in total", futures.size());

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
					if (myPath.toString().endsWith(".json.gz")) {
						try (FileInputStream fileInputStream = new FileInputStream(myPath.toFile())) {
							try (GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream)) {
								try (Reader reader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8)) {
									bundle = IOUtils.toString(reader);
								} catch (IOException e) {
									throw new InternalErrorException(e);
								}
							}
						}
					} else {
						try (FileReader reader = new FileReader(myPath.toFile())) {
							bundle = IOUtils.toString(reader);
						} catch (IOException e) {
							throw new InternalErrorException(e);
						}
					}
					if (isBlank(bundle)) {
						myErrorsCounter.incrementAndGet();
						ourLog.error("Error: empty bundle in {}", myPath.toFile());
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

						ourClientInvocationCounts.get(clientIndex).addInvocation(latency);

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
						ourLog.info("NEXT,{},{},{},{},{},{},{}",
							mySw.getMillis(),
							new DecimalFormat("0.0").format((double) mySw.getMillis() / (1000.0 * 60.0 * 60.0)),
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
							timings.append(" - ");
							timings.append(ourClientInvocationCounts.get(i).describeAndClear());
						}
						ourLog.info(timings.toString());
					}
				} catch (Throwable t) {
					myErrorsCounter.incrementAndGet();
					ourLog.error("Error during task", t);
				}
			}
		}


	}

	private static class ThreadTiming {

		private long myInvocationCount;
		private long myInvocationTime;

		public synchronized void addInvocation(long theLatency) {
			myInvocationCount++;
			myInvocationTime += theLatency;
		}

		public synchronized String describeAndClear() {
			if (myInvocationCount == 0) {
				return "No invocations";
			}

			long average = myInvocationTime / myInvocationCount;
			return myInvocationCount + " files, average " + StopWatch.formatMillis(average) + "/file";
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
			.filter(t -> t.toString().endsWith(".json") || t.toString().endsWith(".json.gz"))
			.sorted(Comparator.comparing(Path::toString))
			.collect(Collectors.toList());

		if (files.size() < 10) {
			throw new Exception(files.size() + " .json files found in " + directory);
		}

		if (ourOffset > 0) {
			ourLog.info("Starting at offset {}", ourOffset);
			files = files.subList(ourOffset, files.size());
		} else {
			ourLog.info("Offset is {}", ourOffset);
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
			ourClientInvocationCounts.add(new ThreadTiming());
		}

		if (uploadMetadata.equals("true")) {
			ourLog.info("Loading metadata files...");
			List<Path> meta = files.stream().filter(t -> t.toString().contains("hospital") || t.toString().contains("practitioner")).collect(Collectors.toList());
			new Uploader(meta);
		}

		ourLog.info("Loading non metadata files...");
		List<Path> nonMeta = files.stream().filter(t -> !t.toString().contains("hospital") && !t.toString().contains("practitioner")).collect(Collectors.toList());

		ourLog.info("Starting real load of {} files with {} threads...", nonMeta.size(), ourMaxThreads);
		new Uploader(nonMeta);
	}


}
