package com.forteanuncio.prep.dadospublicoscnpj.writers;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.constants.FieldsModels;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class WriterSocio extends WriterDefault{

	private Map<String, String> properties;
	private String tableName;

	@Override
	public String getTableName(){
		return tableName;
	}

	public WriterSocio(Map<String, String> properties){
		this.properties = properties;
	}

    public void socioByCnpj() throws Exception{
        String[] fields = FieldsModels.SOCIO;
		String createTable = "CREATE TABLE dadosabertos.sociobycnpj                 (cnpj bigint, codigopais text, codigoqualificacaorepresentantelegal text, percentualCapitalSocial float, codigoqualificacaosocio text, cpfcnpjsocio text, cpfrepresentantelegal text, dataentradasociedade date, identificadorsocio int, nomepaissocio text, nomerepresentantelegal text, nomesocio text, dataHoraInsercao timestamp, PRIMARY KEY (cnpj, dataHoraInsercao) ) WITH CLUSTERing ORDER BY (dataHoraInsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.sociobycnpj ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "sociobycnpj", fields, "socios");
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
			folderWriter += tabela;
			
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
					//skip header
					br.readLine();

					while(br.ready()){
                        ssTableWriter.addRow(SSTableConverter.mappingSociosByString(br.readLine().split("\",\"")));
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