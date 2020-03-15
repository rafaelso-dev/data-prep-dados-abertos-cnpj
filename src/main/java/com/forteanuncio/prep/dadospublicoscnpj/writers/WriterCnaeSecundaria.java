package com.forteanuncio.prep.dadospublicoscnpj.writers;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.constants.FieldsModels;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class WriterCnaeSecundaria extends WriterDefault{

	Map<String,String> properties;
	private String tableName;

	@Override
	public String getTableName(){
		return tableName;
	}

	public WriterCnaeSecundaria(Map<String, String> properties){
		this.properties = properties;
	}

    public void cnaeSecundaria() throws Exception{
        String[] fields = FieldsModels.CNAESECUNDARIA;
		String createTable = "create table dadosabertos.cnaeSecundaria       (cnpj bigint, cnaeSecundaria int, datahorainsercao timestamp, PRIMARY KEY(cnaeSecundaria, datahorainsercao)) WITH CLUSTERING ORDER BY(datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.cnaeSecundaria ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "cnaesecundaria", fields, "cnaes");
    }

    public void cnaeSecundariaByCnpj() throws Exception{
        String[] fields = FieldsModels.CNAESECUNDARIA;
		String createTable = "create table dadosabertos.cnaeSecundariaByCnpj       (cnpj bigint, cnaeSecundaria int, datahorainsercao timestamp, PRIMARY KEY(cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.cnaeSecundariaByCnpj ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "cnaesecundariabycnpj", fields, "cnaes");
    }

	@Override
    public void defaultWriter(String createTable, String insertCommand, String tabela, String[] fields, String parentFolder) throws Exception{
        
		for(int i=0; i < fields.length; i++){
			insertCommand+="?,";
		}
		insertCommand = insertCommand.substring(0, insertCommand.length()-1)+")";
		
		System.out.println("Initializing Writer for table "+tabela);
		
		String folderReader = null;
		String folderWriter = null;
		
		if (isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura."+parentFolder))
			&& isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita."+parentFolder))) {
			this.tableName = tabela;
			folderWriter+=tabela;
			new File(folderWriter).mkdirs();
			try {
				CQLSSTableWriter ssTableWriter  = CQLSSTableWriter
                                    .builder()
                                    .forTable(createTable)
                                    .inDirectory(folderWriter)
                                    .using(insertCommand.toString())
									.withBufferSizeInMB(384)
									.build();
									
				File directory = new File(folderReader);
				for(File file : directory.listFiles()){
					System.out.println("Iniciando "+file.getName());
                    BufferedReader br = new BufferedReader(new FileReader(file), 819200);
                    br.readLine();
					while(br.ready()){
                        String[] arrayCnaes = br.readLine().split(",");
                        for(int i=1; i < arrayCnaes.length; i++){
                            String[] novoArray = new String[arrayCnaes.length-1];
                            if(!arrayCnaes[i].equals("0")){
                                novoArray[0] = arrayCnaes[0]; // cnpj
                                novoArray[1] = arrayCnaes[i]; // novo cnae
                                ssTableWriter.addRow(SSTableConverter.mappingCnaesSecundariaByString(novoArray));
                            }
                        }
						
					}
					br.close();
					ssTableWriter.close();
					System.out.println("Finalizando "+file.getName());
				}
			}catch(Exception e){
				System.out.println("Error on Application. Details: "+e.getMessage()+", Cause: "+e.getCause()+", StackTrace: "+e.getStackTrace());
			}
		}
	}
    
}