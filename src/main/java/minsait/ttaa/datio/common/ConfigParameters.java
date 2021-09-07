package minsait.ttaa.datio.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class ConfigParameters implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private InputStream inputStream;
	public String rutaIn;
	public String rutaOut;
	
		public ConfigParameters() throws IOException {
		try {
			Properties prop = new Properties();
			String propFileName = "config.properties";
			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException(
						"Archivo de configuraciones '" + propFileName + "' no se ha podido encontrar");
			}
			rutaIn = prop.getProperty(Common.INPUT_PATH);
			rutaOut = prop.getProperty(Common.OUTPUT_PATH);
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
	}

	public String getRutaIn() {
		return rutaIn;
	}

	public String getRutaOut() {
		return rutaOut;
	}

}
