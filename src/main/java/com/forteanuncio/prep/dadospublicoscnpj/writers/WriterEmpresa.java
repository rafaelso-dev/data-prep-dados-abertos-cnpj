package com.forteanuncio.prep.dadospublicoscnpj.writers;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.constants.FieldsModels;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterEmpresa{
	
	private static final Logger logger = LoggerFactory.getLogger(WriterEmpresa.class);
	
	public void empresaByNomeFantasiaCnpjDataHoraInsercao(){
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByNomeFantasiaCnpjDataHoraInsercao    (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(nomefantasia, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByNomeFantasiaCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresaByNomeFantasiaCnpjDataHoraInsercao", fields);
    }

	public void empresaByCnaeCnpjDataHoraInsercao(){
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByCnaeCnpjDataHoraInsercao            (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(cnaefiscal, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByCnaeCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresaByCnaeCnpjDataHoraInsercao", fields);
	}

	public void empresaByCnaeRazaoSocialCnpjDataHoraInsercao(){
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByCnaeRazaoSocialCnpjDataHoraInsercao (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(cnaefiscal, razaosocial, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(razaosocial ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByCnaeRazaoSocialCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresaByCnaeRazaoSocialCnpjDataHoraInsercao", fields);
	}

	public void empresaByUfMunicipioCnpjDataHoraInsercao(){
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByUfMunicipioCnpjDataHoraInsercao     (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(uf, municipio, cnpj, datahorainsercao) )WITH CLUSTERING ORDER BY(municipio ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByUfMunicipioCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresaByUfMunicipioCnpjDataHoraInsercao", fields);
	}

	public void empresaByUfCnaeCnpjDataHoraInsercao(){
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByUfCnaeCnpjDataHoraInsercao          (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(uf, cnaefiscal, cnpj, datahorainsercao) )WITH CLUSTERING ORDER BY(cnaefiscal ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByUfCnaeCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresaByUfCnaeCnpjDataHoraInsercao", fields);
	}

	public void defaultWriter(String createTable, String insertCommand, String tabela, String[] fields){
		for(int i=0; i < fields.length; i++){
			insertCommand+="?,";
		}
		insertCommand = insertCommand.substring(0, insertCommand.length()-1)+")";
		
		logger.info("Initializing Writer for table "+tabela);

		logger.info("Loading Properties");
		Map<String, String>  properties = loadProperties();
		logger.info("Properties Loaded");
		
		String folderReader = null;
		String folderWriter = null;
		
		if (isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura.empresas"))
			&& isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita.empresas"))) {
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
									
				//ler a pasta
				File directory = new File(folderReader);
				for(File file : directory.listFiles()){
					logger.info("Iniciando "+file.getName());
					BufferedReader br;
					
					br = new BufferedReader(new FileReader(file), 819200);
					// int conta=0;
					while(br.ready()){
						// conta++;
						String[] arrayValues = br.readLine().split("\",\"");
						// arrayValues[0]=arrayValues[0].replace("\"", "");
						// arrayValues[arrayValues.length-1]=arrayValues[arrayValues.length-1].replace("\"", "");
						// if(arrayValues.length < 38){
						// 	logger.error("Error Parser at line : {} ", line);
						// 	br.close();
						// 	throw new Exception("Error of parser at line : "+line);
						// }
						ssTableWriter.addRow(SSTableConverter.mappingEmpresaByString(arrayValues));
					}
					// logger.info("qtd registros para {} : {}",file.getName(),conta);
					br.close();
					ssTableWriter.close();
					logger.info("Finalizando "+file.getName());
				}
			}catch(Exception e){
				logger.error("Error on Application. Details: {}, Cause: {}, StackTrace {}", 
								e.getMessage(), e.getCause(), e.getStackTrace());
			}
		}
	}

}