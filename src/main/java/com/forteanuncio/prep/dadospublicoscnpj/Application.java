package com.forteanuncio.prep.dadospublicoscnpj;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;
import com.forteanuncio.prep.dadospublicoscnpj.models.Empresa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

	public static List<String> listLinesManaged = new ArrayList<String>();
	public static List<String> listKeysBlocked = new ArrayList<String>();
	
	public static ConcurrentMap<String, List<List<Object>>> mapManaged = new ConcurrentHashMap<String, List<List<Object>>>();

	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	public static boolean existsReaders = true;
	public static boolean existsMappers = true;
	public static boolean existWriters = true;
	
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
				ReaderManager reader = new ReaderManager(folderReader) {};
				Thread readerManager = new Thread(reader);
				readerManager.start();
				
				MapperManager<Empresa> mapper = new MapperManager<Empresa>() {};
				Thread mapperManager = new Thread(mapper);
				mapperManager.start();
				
				File outputDirectory = new File(folderWriter);
				if(!outputDirectory.exists()){
					logger.info("Directory {} not exists, try creating Directory...", outputDirectory);
					outputDirectory.mkdirs();
				}
			
				WriterManager<Empresa> writer = new WriterManager<Empresa>(folderWriter){};
				Thread writerManager = new Thread(writer);
				writerManager.start();
				
				while (existsReaders || existsMappers || existWriters) {
					//16min e 40 segundos
					Thread.sleep(1000);
				}
				
				logger.info("Total de arquivos gerados {}",new File(folderWriter).list().length/8);

			} catch (InterruptedException e) {
				logger.error("Error on try wait for readerExecutors finish. Details: {}", e.getMessage());
			}
		}

		logger.info("Application finished.");
		System.exit(0);

	}

	public static synchronized void addItemOnListLinesManaged(String item){
		Application.listLinesManaged.add(item);
	}

	public static synchronized String removeFirstItemFromListLinesManaged(){
        return Application.listLinesManaged.remove(0);
	}
	
    public static synchronized List<List<Object>> removeKeyFromMapManaged(String key){
		return Application.mapManaged.remove(key);
	}
	
    public static synchronized void addListWithKeyOnMapManaged(String key, List<List<Object>> listItems){
        Application.mapManaged.put(key, listItems);
	}
	
    public static synchronized void addItemOnListWitKeyOnMapManaged(String key, List<Object> item){
        Application.mapManaged.get(key).add(item);
	}

	public static synchronized void addKeyOnListKeysBlocked(String key){
		Application.listKeysBlocked.add(key);
	}

	public static synchronized String removeFirstItemFromListKeysBlocked(){
        return Application.listKeysBlocked.remove(0);
    }
	
}
