package com.forteanuncio.prep.dadospublicoscnpj;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import com.forteanuncio.prep.dadospublicoscnpj.agents.Monitor;
import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;
import com.forteanuncio.prep.dadospublicoscnpj.models.Empresa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	
	public static boolean existsReaders = true;
	public static boolean existsMappers = true;
	public static boolean existWriters = true;
	public static List<String> listLinesManaged = new CopyOnWriteArrayList<String>();
	public static List<String> listKeysBlocked = new CopyOnWriteArrayList<String>();
	public static ConcurrentMap<String, List<List<Object>>> mapManaged = new ConcurrentHashMap<String, List<List<Object>>>();
	
	public static void main(String[] args) {
		logger.info("Initializing Application");

		logger.info("Loading Properties");
		Map<String, String> properties = loadProperties();
		logger.info("Properties Loaded");
		
		String folderReader = null;
		String folderWriter = null;
		
		if (isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura.empresas"))
			&& isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita.empresas"))) {
			
			try {

				

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
}
