package org.mskcc.cmo;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.shared.SampleMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 *
 * @author DivyaMadala
 */

@Configuration
public class PublishNewSample {

	private static int numOfSamples;

	private static String fileName;

	@Value("${igo_new_sample}")
	private static String topic;

	@Autowired
	private static Gateway messagingGateway;

	private static List<SampleMetadata> meta;

	private static void parseArgs(String[] args) {
		List<String> argList = new ArrayList<String>(Arrays.asList(args));
		Iterator<String> argIterator = argList.iterator();

		while (argIterator.hasNext()) {
			String next = argIterator.next();
			switch (next) {
			case "-n":
				numOfSamples = Integer.parseInt(argIterator.next());
				argIterator.remove();
				continue;
			case "-f":
				fileName = argIterator.next();
				argIterator.remove();
				continue;
			}
		}
	}

	static void getData() {
		try {
			Reader reader = Files.newBufferedReader(Paths.get(fileName));
			meta = new Gson().fromJson(reader, new TypeToken<List<SampleMetadata>>() {
			}.getType());
			// add check for number of samples?
			reader.close();
		} catch (JsonIOException | JsonSyntaxException | IOException e) {
			e.printStackTrace();
		}
	}

	static void publishData() {
		try {
			getData();
			for (SampleMetadata s : meta) {
				messagingGateway.publish(topic, s.toString().getBytes());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args != null | args.length >= 1) {
			parseArgs(args);
		}
		publishData();
	}

}