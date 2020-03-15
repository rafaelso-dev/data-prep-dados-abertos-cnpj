package com.forteanuncio.prep.dadospublicoscnpj.writers;

import com.forteanuncio.prep.dadospublicoscnpj.constants.FieldsModels;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class WriterEmpresa extends WriterDefault{
	
	private Map<String, String> properties;
	private String tableName;

	@Override
	public String getTableName(){
		return tableName;
	}
	
	public WriterEmpresa(Map<String, String>  properties){
		this.properties = properties;
	}

	public void empresaByNomeFantasiaCnpjDataHoraInsercao() throws Exception{
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByNomeFantasiaCnpjDataHoraInsercao    (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(nomefantasia, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByNomeFantasiaCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresabynomefantasiacnpjdatahorainsercao", fields, "empresa");
    }

	public void empresaByCnaeCnpjDataHoraInsercao() throws Exception{
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByCnaeCnpjDataHoraInsercao            (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(cnaefiscal, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByCnaeCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresabycnaecnpjdatahorainsercao", fields, "empresa");
	}

	public void empresaByCnaeRazaoSocialCnpjDataHoraInsercao() throws Exception{
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByCnaeRazaoSocialCnpjDataHoraInsercao (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(cnaefiscal, razaosocial, cnpj, datahorainsercao)) WITH CLUSTERING ORDER BY(razaosocial ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByCnaeRazaoSocialCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresabycnaerazaosocialcnpjdatahorainsercao", fields, "empresa");
	}

	public void empresaByUfMunicipioCnpjDataHoraInsercao() throws Exception{
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByUfMunicipioCnpjDataHoraInsercao     (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(uf, municipio, cnpj, datahorainsercao) )WITH CLUSTERING ORDER BY(municipio ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByUfMunicipioCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresabyufmunicipiocnpjdatahorainsercao", fields, "empresa");
	}

	public void empresaByUfCnaeCnpjDataHoraInsercao() throws Exception{
		String[] fields = FieldsModels.EMPRESA;
		String createTable = "CREATE TABLE dadosabertos.empresaByUfCnaeCnpjDataHoraInsercao          (cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY(uf, cnaefiscal, cnpj, datahorainsercao) )WITH CLUSTERING ORDER BY(cnaefiscal ASC, cnpj ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.05 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '32', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 0 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 30000 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";
		String insertCommand = "INSERT INTO dadosabertos.empresaByUfCnaeCnpjDataHoraInsercao ("+String.join(",", fields).toLowerCase() +") values (";
		defaultWriter(createTable, insertCommand, "empresabyufcnaecnpjdatahorainsercao", fields, "empresa");
	}

	@Override
	public void defaultWriter(String createTable, String insertCommand, String tabela, String[] fields, String parentFolder) throws Exception{
        this.tableName = tabela;
		for(int i=0; i < fields.length; i++){
			insertCommand+="?,";
		}
		insertCommand = insertCommand.substring(0, insertCommand.length()-1)+")";
		
		System.out.println("Initializing Writer for table "+tabela);

		String folderReader = null;
		String folderWriter = null;
		
		if (isNotNullAndIsNotEmpty(folderReader = properties.get("pasta.leitura."+parentFolder))
			&& isNotNullAndIsNotEmpty(folderWriter = properties.get("pasta.escrita."+parentFolder))) {
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
					while(br.ready()){
						ssTableWriter.addRow(SSTableConverter.mappingEmpresaByString(br.readLine().split("\",\"")));
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