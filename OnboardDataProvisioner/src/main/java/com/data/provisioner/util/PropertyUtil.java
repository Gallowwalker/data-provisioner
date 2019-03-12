package com.data.provisioner.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.Properties;

import com.data.provisioner.vehicle.api.Vehicle;

public class PropertyUtil {
	
	private static WatchService watchService = null;
	
	private PropertyUtil() {
		
	}
	
	public static void listenForChanges(final String directoryPath, final Vehicle vehicle) throws IOException, InterruptedException {
 		try {
 			PropertyUtil.watchService = FileSystems.getDefault().newWatchService();
 			Paths.get(directoryPath).register(
 					PropertyUtil.watchService, 
				StandardWatchEventKinds.ENTRY_CREATE,
				StandardWatchEventKinds.ENTRY_DELETE,
				StandardWatchEventKinds.ENTRY_MODIFY
 			);
 			
 			WatchKey key;
 			while ((key = PropertyUtil.watchService.take()) != null) {
 				for (final WatchEvent<?> event : key.pollEvents()) {
 					final Kind eventKind = event.kind();
 					if (eventKind == StandardWatchEventKinds.ENTRY_MODIFY) {
 						vehicle.restart("Configuration values were altered. Applying the new ones.");
 					}
//	               System.out.println(
//	                 "Event kind:" + event.kind() 
//	                   + ". File affected: " + event.context() + ".");
 				}
 				key.reset();
 			}
 		} finally {
 			PropertyUtil.stopListeningForChanges();
 		}
	}
	
	public static void stopListeningForChanges() throws IOException {
		if (PropertyUtil.watchService != null) {
			PropertyUtil.watchService.close();
		}
	}
	
	public static Properties loadProperties(final String resource) throws IOException {
		final Properties properties = new Properties();
		try (
			final InputStream fileInputStream = Objects.requireNonNull(new FileInputStream(resource), "Can't load resource - " + resource);
		) {
			properties.load(fileInputStream);
		}
		return properties;
	}

}
