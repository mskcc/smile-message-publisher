package org.mskcc.cmo;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.shared.SampleMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.gson.Gson;

/**
 *
 * @author DivyaMadala
 */
@SpringBootApplication(scanBasePackages = "org.mskcc.cmo.messaging")
public class NewSamplePublisher implements CommandLineRunner {

	@Autowired
	private Gateway messagingGateway;

	private static String fileName;

	@Value("${igo_new_sample}")
	private static String topic;

	public static void main(String[] args) throws Exception {
		SpringApplication.run(NewSamplePublisher.class, args);
	}

	public SampleMetadata readFile() {
		try {
			Gson gson = new Gson();
			Reader reader = Files.newBufferedReader(Paths.get(fileName));
		    SampleMetadata s = gson.fromJson(reader,SampleMetadata.class);
			return s;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void run(String... args) throws Exception {
		fileName = args[0];
		messagingGateway.connect();
		try {
			System.out.println("Publishing new sample");
			SampleMetadata s = readFile();
			messagingGateway.publish(topic, s.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
