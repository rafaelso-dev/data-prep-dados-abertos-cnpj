package com.forteanuncio.prep.dadospublicoscnpj.converters;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.datastax.driver.core.LocalDate;

public class SSTableConverter<T> {
    
    private static final String EMPTY="";
    private static final String ERRORDATE="00000000";
    
    public static List<Object> mappingEmpresaByString(String[] values){
        List<Object> listaCampos = new ArrayList<Object>();
        // Long cnpj
        listaCampos.add(EMPTY.equals(values[0].replaceAll("[^0-9]", "")) ? null : new Long(values[0].replaceAll("[^0-9]", "")) );
        // Integer matriz
        listaCampos.add(EMPTY.equals(values[1].replaceAll("[^0-9]", "")) ? null : new Integer(values[1].replaceAll("[^0-9]", "")) );
        // String razaoSocial
        listaCampos.add(EMPTY.equals(values[2].trim()) ? "N/A" : values[2] );
        // String nomeFantasia
        listaCampos.add(EMPTY.equals(values[3].trim()) ? "N/A" : values[3] );
        // Integer situacaoCadastral
        listaCampos.add(EMPTY.equals(values[4].replaceAll("[^0-9]", "")) ? null : new Integer(values[4].replaceAll("[^0-9]", "")) );
        // LocalDate dataSituacaoCadastral
        listaCampos.add(values[5].length() == 8 && !values[5].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(values[5].substring(0,4)), 
                                       new Integer(values[5].substring(4,6)), 
                                       new Integer(values[5].substring(6,8))
            ) : 
            null
        );
        // Integer codigoMotivoSitualCadastral
        listaCampos.add(EMPTY.equals(values[6].replaceAll("[^0-9]", "")) ? null : new Integer(values[6].replaceAll("[^0-9]", "")) );
        // String nomeCidadeExterior
        listaCampos.add(EMPTY.equals(values[7].trim()) ? null : values[7] );
        // String codigoPais
        listaCampos.add(EMPTY.equals(values[8].trim()) ? null : values[8] );
        // String nomePais
        listaCampos.add(EMPTY.equals(values[9].trim()) ? null : values[9] );
        // Integer codigoNaturezaJuridica
        listaCampos.add(EMPTY.equals(values[10].replaceAll("[^0-9]", "")) ? null : new Integer(values[10].replaceAll("[^0-9]", "")) );
        // LocalDate dataInicioAtividade
        listaCampos.add(values[11].length() == 8 && !values[11].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(values[11].substring(0,4)), 
                                       new Integer(values[11].substring(4,6)), 
                                       new Integer(values[11].substring(6,8))
            ) : 
            null
        );
        // Integer cnaeFiscal
        listaCampos.add(EMPTY.equals(values[12].replaceAll("[^0-9]", "")) ? null : new Integer(values[12].replaceAll("[^0-9]", "")) );

        // String descLogradouro
        listaCampos.add(EMPTY.equals(values[13].trim()) ? null : values[13] );

        // String logradouro
        listaCampos.add(EMPTY.equals(values[14].trim()) ? null : values[14] );

        // String numero
        listaCampos.add(EMPTY.equals(values[15].trim()) ? null : values[15]);

        // String complemento
        listaCampos.add(EMPTY.equals(values[16].trim()) ? null : values[16] );

        // String bairro
        listaCampos.add(EMPTY.equals(values[17].trim()) ? null : values[17] );

        // Integer cep
        listaCampos.add(EMPTY.equals(values[18].replaceAll("[^0-9]", "")) ? null : new Integer(values[18].replaceAll("[^0-9]", "")) );

        // String uf
        listaCampos.add(EMPTY.equals(values[19].trim()) ? null : values[19] );

        // Integer codigoMunicipio
        listaCampos.add(EMPTY.equals(values[20].replaceAll("[^0-9]", "")) ? null : new Integer(values[20].replaceAll("[^0-9]", "")) );

        // String municipio
        listaCampos.add(EMPTY.equals(values[21].trim()) ? null : values[21] );

        // Integer ddd1
        listaCampos.add(EMPTY.equals(values[22].replaceAll("[^0-9]", "")) ? null : new Integer(values[22].replaceAll("[^0-9]", "")) );

        // Integer telefone1
        listaCampos.add(EMPTY.equals(values[23].replaceAll("[^0-9]", "")) ? null : new Integer(values[23].replaceAll("[^0-9]", "")) );

        // Integer ddd2
        listaCampos.add(EMPTY.equals(values[24].replaceAll("[^0-9]", "")) ? null : new Integer(values[24].replaceAll("[^0-9]", "")) );
        
        // Integer telefone2
        listaCampos.add(EMPTY.equals(values[25].replaceAll("[^0-9]", "")) ? null : new Integer(values[25].replaceAll("[^0-9]", "")) );

        // Integer dddFax
        listaCampos.add(EMPTY.equals(values[26].replaceAll("[^0-9]", "")) ? null : new Integer(values[26].replaceAll("[^0-9]", "")) );

        // Integer numFax
        listaCampos.add(EMPTY.equals(values[27].replaceAll("[^0-9]", "")) ? null : new Integer(values[27].replaceAll("[^0-9]", "")) );

        // String email
        listaCampos.add(EMPTY.equals(values[28].trim()) ? null : values[28] );

        // Integer qualificacaoResponsavel
        listaCampos.add(EMPTY.equals(values[29].replaceAll("[^0-9]", "")) ? null : new Integer(values[29].replaceAll("[^0-9]", "")) );

        // Float capitalSocial
        listaCampos.add(EMPTY.equals(values[30]) ? null : new Float(values[30]) );

        // String porteEmpresa
        listaCampos.add(EMPTY.equals(values[31].trim()) ? null : values[31] );

        // String opcaoPeloSimplesNacional
        listaCampos.add(EMPTY.equals(values[32].trim()) ? null : values[32] );

        // LocalDate dataOpcaoPeloSimplesNacional
        listaCampos.add(values[33].length() == 8 && !values[33].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(values[33].substring(0,4)), 
                                       new Integer(values[33].substring(4,6)), 
                                       new Integer(values[33].substring(6,8))
            ) : 
            null
        );

        // LocalDate dataExclusaoSimplesNacional
        listaCampos.add(values[34].length() == 8 && !values[34].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(values[34].substring(0,4)), 
                                       new Integer(values[34].substring(4,6)), 
                                       new Integer(values[34].substring(6,8))
            ) : 
            null
        );

        // String opcaoPeloMei
        listaCampos.add(EMPTY.equals(values[35].trim()) ? null : values[35].trim() );

        // String situacaoEspecial
        listaCampos.add(EMPTY.equals(values[36].trim()) ? null : values[36].trim() );

        // LocalDate dataSituacaoEspecial
        listaCampos.add(values[37].length() == 8 && !values[37].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(values[37].substring(0,4)), 
                                       new Integer(values[37].substring(4,6)), 
                                       new Integer(values[37].substring(6,8))
            ) : 
            null
        );
        
        // LocalDateTime dataHoraInsercao
        listaCampos.add(Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()));
        
        return listaCampos;
    }

    public static List<Object> mappingCnaesSecundariaByString(String[] values){
        
        List<Object> listaCampos = new ArrayList<Object>();
        
        // Long cnae
        listaCampos.add(EMPTY.equals(values[0].replaceAll("[^0-9]", "")) ? null : new Long(values[0].replaceAll("[^0-9]", "")) );
        
        // Integer cnaeSecundaria
        listaCampos.add(EMPTY.equals(values[1].replaceAll("[^0-9]", "")) ? null : new Integer(values[1].replaceAll("[^0-9]", "")) );

        // LocalDateTime dataHoraInsercao
        listaCampos.add(Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()));
                
        return listaCampos;
    }

    public static List<Object> mappingSociosByString(String[] values){
        List<Object> listaCampos = new ArrayList<Object>();
        
        // Long cnpj;
        listaCampos.add(EMPTY.equals(values[0].replaceAll("[^0-9]", "")) ? null : new Long(values[0].replaceAll("[^0-9]", "")) );

        // Integer identificadorSocio;
        // // 1 - Pessoa Juridica
        // // 2 - Pessoa Fisica
        // // 3 - Estrangeiro
        listaCampos.add(EMPTY.equals(values[1].replaceAll("[^0-9]", "")) ? null : new Integer(values[1].replaceAll("[^0-9]", "")) );

        // String nomeSocio;
        listaCampos.add(EMPTY.equals(values[2].trim()) ? "N/A" : values[2].trim() );

        // String cpfCnpjSocio;
        listaCampos.add(EMPTY.equals(values[3].trim()) ? "N/A" : values[3].trim() );
        
        String[] newValues = values[4].split(",");
        
        // String codigoQualificacaoSocio;
        listaCampos.add(EMPTY.equals(newValues[0].substring(0, newValues[0].length()-1).trim()) ? "N/A" : newValues[0].substring(0, newValues[0].length()-1).trim() );

        // Float percentualCapitalSocial;
        listaCampos.add(EMPTY.equals(newValues[1]) ? null : new Float(newValues[1]) );

        // LocalDate dataEntradaSociedade;
        newValues[2] = newValues[2].substring(1);
        listaCampos.add(newValues[2].length() == 8 && !newValues[2].equals(ERRORDATE) ? 
            LocalDate.fromYearMonthDay(new Integer(newValues[2].substring(0,4)), 
                                       new Integer(newValues[2].substring(4,6)), 
                                       new Integer(newValues[2].substring(6,8))
            ) : 
            null
        );

        // String codigoPais;
        listaCampos.add(EMPTY.equals(values[5].trim()) ? "N/A" : values[5].trim() );

        // String nomePaisSocio;
        listaCampos.add(EMPTY.equals(values[6].trim()) ? "N/A" : values[6].trim() );

        // String cpfRepresentanteLegal;
        listaCampos.add(EMPTY.equals(values[7].trim()) ? "N/A" : values[7].trim() );

        // String nomeRepresentanteLegal;
        listaCampos.add(EMPTY.equals(values[8].trim()) ? "N/A" : values[8].trim() );

        // String codigoQualificacaoRepresentanteLegal;
        listaCampos.add(EMPTY.equals(values[9].trim()) ? "N/A" : values[9].trim() );

        // LocalDateTime dataHoraInsercao
        listaCampos.add(Date.from(LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant()));

        return listaCampos;

    }
}