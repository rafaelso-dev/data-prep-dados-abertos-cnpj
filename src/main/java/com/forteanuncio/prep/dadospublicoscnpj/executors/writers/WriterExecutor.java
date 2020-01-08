package com.forteanuncio.prep.dadospublicoscnpj.executors.writers;

import java.util.List;

import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterExecutor implements Runnable {

    Logger logger = LoggerFactory.getLogger(WriterExecutor.class);
    
    private String pathDirectoryWriter;
    private String key;
    List<List<Object>> values;

    public WriterExecutor(String pathDirectoryWriter, String key, List<List<Object>> values) {
        this.pathDirectoryWriter = pathDirectoryWriter;
        this.key = key;
        this.values = values;
    }

    private String createTable = "CREATE TABLE dadosabertos.empresa ( cnpj bigint, razaosocial text, datahorainsercao timestamp, bairro text, capitalsocial float, cep int, cnaefiscal int, codigomotivositualcadastral int, codigomunicipio int, codigonaturezajuridica int, codigopais text, complemento text, dataexclusaosimplesnacional date, datainicioatividade date, dataopcaopelosimplesnacional date, datasituacaocadastral date, datasituacaoespecial date, ddd1 int, ddd2 int, dddfax int, desclogradouro text, email text, logradouro text, matriz int, municipio text, nomecidadeexterior text, nomefantasia text, nomepais text, numero text, numfax int, opcaopelomei text, opcaopelosimplesnacional text, porteempresa text, qualificacaoresponsavel int, situacaocadastral int, situacaoespecial text, telefone1 int, telefone2 int, uf text, PRIMARY KEY (cnpj, razaosocial, datahorainsercao) ) WITH CLUSTERING ORDER BY (razaosocial ASC, datahorainsercao DESC) AND bloom_filter_fp_chance = 0.01 AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'} AND comment = '' AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'} AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'} AND crc_check_chance = 1.0 AND default_time_to_live = 0 AND gc_grace_seconds = 864000 AND max_index_interval = 2048 AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128 AND speculative_retry = '99PERCENTILE';";

    @Override
    public void run() {
        logger.debug("Starting Writer Executor.");
        if(this.values != null){
            try {
                
                String columnsName = key.toLowerCase();
                StringBuilder insertCommand = new StringBuilder("INSERT INTO dadosabertos.empresa (").append(columnsName).append(") values (");
                StringBuilder insertCommandValues = new StringBuilder("");
                
                String[] colunsMap = columnsName.split(",");
                for (int i = 0; i < colunsMap.length; i++) {
                    insertCommandValues.append("?,");
                }
                
                String insertCommandValuesString = insertCommandValues.toString();
                insertCommand.append(insertCommandValuesString.substring(0,insertCommandValuesString.length() - 1)).append(")");
                
                logger.debug("Insert statement generated");

                Builder writer = CQLSSTableWriter
                                    .builder()
                                    .forTable(createTable)
                                    .inDirectory(pathDirectoryWriter)
                                    .using(insertCommand.toString())
                                    .withBufferSizeInMB(256);
                
                CQLSSTableWriter cqlWriter = writer.build();

                logger.debug("cqlWriter constructed");
                
                
                WriterManager.addLines(this.values.size());

                while(!this.values.isEmpty()){
                    cqlWriter.addRow(this.values.remove(0));
                }
                
                logger.debug("writing file on disk with for {} lines.");
                cqlWriter.close();
                
            } catch (Exception ex) {
                logger.error("Error on writer Executer. Cause : {}. Details : {} LocalizedMessage {}. StackTrace {}.", ex.getCause(), ex.getMessage(), ex.getLocalizedMessage(), ex.getStackTrace());
            }
        }
        logger.debug("Finishing Writer Executor");
    }

}