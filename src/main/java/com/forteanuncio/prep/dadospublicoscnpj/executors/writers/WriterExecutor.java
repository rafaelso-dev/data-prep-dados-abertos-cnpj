package com.forteanuncio.prep.dadospublicoscnpj.executors.writers;

import java.io.IOException;
import java.util.List;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;
import com.forteanuncio.prep.dadospublicoscnpj.utils.Utils;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter.Builder;

import java.lang.reflect.ParameterizedType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterExecutor<T> implements Runnable {

    Logger logger = LoggerFactory.getLogger(WriterExecutor.class);
    
    private String pathDirectoryWriter;
    private String genericHeader;
    private Utils<T> utils = new Utils<T>(){};

    private Class<?> clazz;

    public WriterExecutor(String pathDirectoryWriter, String genericHeader,
            List<List<Object>> conteudoArquivo, Class<?> clazz) {
        this.pathDirectoryWriter = pathDirectoryWriter;
        this.genericHeader = genericHeader;
        this.clazz = clazz;
    }

    private String createTableEmpresaPrefix = "CREATE TABLE dadosabertos.empresa (";
    private String createTableEmpresaSufix = " PRIMARY KEY (cnpj,razaosocial,datahorainsercao) ) WITH CLUSTERING ORDER BY (razaosocial ASC,datahorainsercao DESC)  AND bloom_filter_fp_chance = 0.01  AND caching = {'keys': 'ALL','rows_per_partition': 'NONE'}  AND comment = ''  AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold': '32','min_threshold': '4'}  AND compression = {'chunk_length_in_kb': '64','class': 'org.apache.cassandra.io.compress.LZ4Compressor'}  AND crc_check_chance = 1.0  AND default_time_to_live = 0  AND gc_grace_seconds = 864000  AND max_index_interval = 2048  AND memtable_flush_period_in_ms = 0  AND min_index_interval = 128  AND speculative_retry = '99PERCENTILE';";
    
    @Override
    public void run() {
        try {
            logger.info("Iniciando Executor de Gerador de arquivos"); 
            if(clazz == null){
                @SuppressWarnings("unchecked")
                Class<?> clazzz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
                this.clazz = clazzz;
            }
            String [] fieldsType = utils.getFieldsByType(clazz);
            genericHeader = genericHeader.toLowerCase();
            StringBuilder insertCommand = new StringBuilder("INSERT INTO dadosabertos.empresa (").append(genericHeader).append(") values (");
            StringBuilder insertCommandValues = new StringBuilder("");

            String tableEmpresaGenerated = "";
            
            for (int i = 0; i < fieldsType.length; i++) {
                if(genericHeader.contains(fieldsType[i].split("\\s")[0])){
                    tableEmpresaGenerated = tableEmpresaGenerated.concat(fieldsType[i]).concat(",");
                    insertCommandValues.append("?,");
                }
            }

            String insertCommandValuesString = insertCommandValues.toString();
            insertCommand.append(insertCommandValuesString.substring(0,insertCommandValuesString.length() - 1)).append(")");
            
            Builder writer = CQLSSTableWriter
                                .builder()
                                .forTable(createTableEmpresaPrefix + tableEmpresaGenerated + createTableEmpresaSufix)
                                .inDirectory(pathDirectoryWriter)
                                .using(insertCommand.toString())
                                .withBufferSizeInMB(256);

            CQLSSTableWriter cqlWriter = writer.build();
            
            while(!Application.mapManaged.get(genericHeader).isEmpty()){
                cqlWriter.addRow(Application.removeFirstItemFromListOnMapManaged(genericHeader));
                WriterManager.addLine();
            }
            if(Application.mapManaged.get(genericHeader).size() == 0){
                Application.removeKeyFromMapManaged(genericHeader);
                cqlWriter.close();
            } 
            //SSTableLoader sstableLoader = new SSTableLoader(directory, client, outputHandler, 5);
        } catch (IOException ex) {
            ex.printStackTrace();
         
        }
    }

}