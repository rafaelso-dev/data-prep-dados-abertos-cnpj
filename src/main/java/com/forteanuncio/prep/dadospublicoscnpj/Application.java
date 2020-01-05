package com.forteanuncio.prep.dadospublicoscnpj;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;
import com.forteanuncio.prep.dadospublicoscnpj.models.CNAESecundaria;
import com.forteanuncio.prep.dadospublicoscnpj.models.Empresa;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

public class Application {

	public static List<String> listManaged = new ArrayList<String>();
	public static ConcurrentMap<String, List<List<Object>>> mapManaged = new ConcurrentHashMap<String, List<List<Object>>>();

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {

		Map<String, String> properties = loadProperties();
		String folderReader = null;
		String folderWriter = null;
		if (isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura.empresas"))
			&& isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita.empresas"))) {
			
			try {
				ReaderManager reader = new ReaderManager<Empresa>(folderReader) {
				};
				Thread readerManager = new Thread(reader);
				readerManager.start();
				Thread.sleep(100);
				MapperManager mapper = new MapperManager<Empresa>() {};
				Thread mapperManager = new Thread(mapper);
				mapperManager.start();
				Thread.sleep(100);
				WriterManager writer = new WriterManager<Empresa>(folderWriter){};
				Thread writerManager = new Thread(writer);
				writerManager.start();
				Thread.sleep(100);
				while (!reader.executors.isShutdown() || !mapper.executors.isShutdown() || !writer.executors.isShutdown()) {
					Thread.sleep(25);
				}
			} catch (InterruptedException e) {
				logger.error("Error on try wait for readerExecutors finish. Details: {}", e.getMessage());
			}
		}

		if(
			isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura.cnaes")) && 
			isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita.cnaes")) 
		){
			Thread readerManager = new Thread(new ReaderManager<CNAESecundaria>(folderReader){});
			readerManager.start();
	
			Thread mapperManager = new Thread(new MapperManager<CNAESecundaria>(){});
			mapperManager.start();
	
			// Thread writerManager = new Thread(new WriterManager<CNAESecundaria>(folderWriter){});
			// writerManager.start();
		}
		
		if(
			isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura.socios")) && 
			isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita.socios"))
		){
			Thread readerManager = new Thread(new ReaderManager<CNAESecundaria>(folderReader){});
			readerManager.start();
	
			Thread mapperManager = new Thread(new MapperManager<CNAESecundaria>(){});
			mapperManager.start();
	
			// Thread writerManager = new Thread(new WriterManager<CNAESecundaria>(folderWriter){});
			// writerManager.start();
		}

		System.exit(0);

	}

	public static synchronized void addItemTolistManaged(String item){
		listManaged.add(item);
	}

	public static synchronized String removeFirstItemFromManaged(){
        return listManaged.remove(0);
    }
    public static synchronized List<Object> removeFirstItemFromListOnMapManaged(String key){
        return mapManaged.get(key).remove(0);
    }
    public static synchronized void removeKeyFromMapManaged(String key){
        mapManaged.remove(key);
    }
    public static synchronized void addListWithKeyOnMapManaged(String key, List<List<Object>> listItems){
        mapManaged.put(key, listItems);
    }
    public static synchronized void addItemOnListWitKeyOnMapManaged(String key, List<Object> item){
        mapManaged.get(key).add(item);
	}
	
}
