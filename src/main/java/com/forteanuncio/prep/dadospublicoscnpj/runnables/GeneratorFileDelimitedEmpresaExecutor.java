package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.LocalDate;
import com.forteanuncio.prep.dadospublicoscnpj.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.model.Empresa;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorFileDelimitedEmpresaExecutor implements Runnable {

    Logger logger = LoggerFactory.getLogger(GeneratorFileDelimitedEmpresaExecutor.class);
    
    private String pathDirectoryWriter;
    private String genericHeader;
    private List<String> conteudoArquivo;

    public GeneratorFileDelimitedEmpresaExecutor(String pathDirectoryWriter, String genericHeader,
            List<String> conteudoArquivo) {
        this.pathDirectoryWriter = pathDirectoryWriter;
        this.genericHeader = genericHeader;
        this.conteudoArquivo = conteudoArquivo;
    }

    private String createTableEmpresa = "CREATE TABLE dadosabertos.empresa (  cnpj bigint,  razaosocial text,  datahorainsercao timestamp,  bairro text,  capitalsocial float,  cep int,  cnaefiscal int,  codigomotivositualcadastral int,  codigomunicipio int,  codigonaturezajuridica int,  codigopais text,  complemento text,  dataexclusaosimplesnacional date,  datainicioatividade date,  dataopcaopelosimplesnacional date,  datasituacaocadastral date,  datasituacaoespecial date,  ddd1 int,  ddd2 int,  dddfax int,  desclogradouro text,  email text,  logradouro text,  matriz int,  municipio text,  nomecidadeexterior text,  nomefantasia text,  nomepais text,  numero text,  numfax int,  opcaopelomei text,  opcaopelosimplesnacional text,  porteempresa text,  qualificacaoresponsavel int,  situacaocadastral int,  situacaoespecial text,  telefone1 int,  telefone2 int,  uf text,  PRIMARY KEY (cnpj, razaosocial, datahorainsercao) ) WITH CLUSTERING ORDER BY (razaosocial ASC, datahorainsercao DESC)  AND bloom_filter_fp_chance = 0.01  AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}  AND comment = ''  AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}  AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}  AND crc_check_chance = 1.0  AND default_time_to_live = 0  AND gc_grace_seconds = 864000  AND max_index_interval = 2048  AND memtable_flush_period_in_ms = 0  AND min_index_interval = 128  AND speculative_retry = '99PERCENTILE';";
    private String separatorCaracter="\\{";

    @Override
    public void run() {
        try {

            StringBuilder insertCommand = new StringBuilder("INSERT INTO dadosabertos.empresa (").append(genericHeader).append(") values (");
            StringBuilder insertCommandValues = new StringBuilder("");

            String[] fields = genericHeader.split(",");
            for (int i = 0; i < fields.length; i++) {
                insertCommandValues.append("?,");
            }

            String insertCommandValuesString = insertCommandValues.toString();
            insertCommand.append(insertCommandValuesString.substring(0, insertCommandValuesString.length() - 1)).append(")");
            
            Builder writer = CQLSSTableWriter
                                .builder()
                                .forTable(createTableEmpresa)
                                .inDirectory(pathDirectoryWriter)
                                .using(insertCommand.toString())
                                .withBufferSizeInMB(256);

            CQLSSTableWriter cqlWriter = writer.build();
            
            List<Object> listaCampos = new ArrayList<Object>();
            
            for (int k = 0; k < conteudoArquivo.size(); k++) {
                String line = conteudoArquivo.get(k);
                String[] fieldMapper = genericHeader.split(",");
                String[] valueMapper = line.split(separatorCaracter);
                for(int i = 0; i < fieldMapper.length; i++){
                    String campo = fieldMapper[i];
                    String value = valueMapper[i];
                    classes : for (Field f : Empresa.class.getDeclaredFields()) {
                                if (f.getName().toLowerCase().equals(campo)) {
                                    switch(f.getType().getSimpleName()){
                                        case "Long":
                                            listaCampos.add(Long.valueOf(value));
                                            break;
                                        case "Integer":
                                            listaCampos.add(Integer.valueOf(value));
                                            break;
                                        case "LocalDateTime":
                                            String[] localdateTime = value.split("T");
                                            String[] data = localdateTime[0].split("-");
                                            String[] hora = localdateTime[1].split(":");
                                            if(hora[2].split("\\.").length > 1){
                                                listaCampos.add(Date.from(LocalDateTime.of(
                                                    Integer.parseInt(data[0]),
                                                    Integer.parseInt(data[1]),
                                                    Integer.parseInt(data[2]),
                                                    Integer.parseInt(hora[0]),
                                                    Integer.parseInt(hora[1]),
                                                    Integer.parseInt(hora[2].split("\\.")[0]),
                                                    Integer.parseInt(hora[2].split("\\.")[1])*1000).atZone(ZoneId.systemDefault()).toInstant()));
                                            }else{
                                                listaCampos.add(Date.from(LocalDateTime.of(
                                                Integer.parseInt(data[0]),
                                                Integer.parseInt(data[1]),
                                                Integer.parseInt(data[2]),
                                                Integer.parseInt(hora[0]),
                                                Integer.parseInt(hora[1]),
                                                Integer.parseInt(hora[2].split("\\.")[0])).atZone(ZoneId.systemDefault()).toInstant()));
                                            }
                                            
                                            break;
                                        case "LocalDate":
                                            String[] localDate = value.split("-");
                                            listaCampos.add(LocalDate.fromYearMonthDay(
                                                Integer.parseInt(localDate[0]),
                                                Integer.parseInt(localDate[1]),
                                                Integer.parseInt(localDate[2])));
                                            break;
                                        case "Float":
                                            listaCampos.add(Float.valueOf(value));
                                            break;
                                        default:
                                            listaCampos.add(value);
                                            break;
                                    }
                                    break classes;
                                }
                    }
                }
                cqlWriter.addRow(listaCampos);
                GeneratorFileDelimitedEmpresa.addLine();
            }
            cqlWriter.close();
            
        } catch (IOException ex) {
            ex.printStackTrace();
         
        }
    }

}