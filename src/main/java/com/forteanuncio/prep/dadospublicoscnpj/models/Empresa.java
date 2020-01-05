package com.forteanuncio.prep.dadospublicoscnpj.models;

import java.io.Serializable;
import java.time.LocalDateTime;

import com.datastax.driver.core.LocalDate;
import com.forteanuncio.prep.dadospublicoscnpj.annotations.MappedFieldFileWithColumnCassandra;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class Empresa implements Serializable{
    
    private static final long serialVersionUID = -6634755854220896813L;

    @MappedFieldFileWithColumnCassandra(1)
    private Long cnpj;
    
    @MappedFieldFileWithColumnCassandra(2)
    private Integer matriz;

    @MappedFieldFileWithColumnCassandra(3)
    private String razaoSocial;

    @MappedFieldFileWithColumnCassandra(4)
    private String nomeFantasia;
    
    @MappedFieldFileWithColumnCassandra(5)
    private Integer situacaoCadastral;
    
    @MappedFieldFileWithColumnCassandra(6)
    private LocalDate dataSituacaoCadastral;
    
    @MappedFieldFileWithColumnCassandra(7)
    private Integer codigoMotivoSitualCadastral;
    
    @MappedFieldFileWithColumnCassandra(8)
    private String nomeCidadeExterior;
    
    @MappedFieldFileWithColumnCassandra(9)
    private String codigoPais;
    
    @MappedFieldFileWithColumnCassandra(10)
    private String nomePais;
    
    @MappedFieldFileWithColumnCassandra(11)
    private Integer codigoNaturezaJuridica;
    
    @MappedFieldFileWithColumnCassandra(12)
    private LocalDate dataInicioAtividade;
    
    @MappedFieldFileWithColumnCassandra(13)
    private Integer cnaeFiscal;
    
    @MappedFieldFileWithColumnCassandra(14)
    private String descLogradouro;
    
    @MappedFieldFileWithColumnCassandra(15)
    private String logradouro;

    @MappedFieldFileWithColumnCassandra(16)
    private String numero;

    @MappedFieldFileWithColumnCassandra(17)
    private String complemento;

    @MappedFieldFileWithColumnCassandra(18)
    private String bairro;

    @MappedFieldFileWithColumnCassandra(19)
    private Integer cep;

    @MappedFieldFileWithColumnCassandra(20)
    private String uf;

    @MappedFieldFileWithColumnCassandra(21)
    private Integer codigoMunicipio;

    @MappedFieldFileWithColumnCassandra(22)
    private String municipio;

    @MappedFieldFileWithColumnCassandra(23)
    private Integer ddd1;

    @MappedFieldFileWithColumnCassandra(24)
    private Integer telefone1;
    
    @MappedFieldFileWithColumnCassandra(25)
    private Integer ddd2;
    
    @MappedFieldFileWithColumnCassandra(26)
    private Integer telefone2;
    
    @MappedFieldFileWithColumnCassandra(27)
    private Integer dddFax;
    
    @MappedFieldFileWithColumnCassandra(28)
    private Integer numFax;

    @MappedFieldFileWithColumnCassandra(29)
    private String email;
    
    @MappedFieldFileWithColumnCassandra(30)
    private Integer qualificacaoResponsavel;

    @MappedFieldFileWithColumnCassandra(31)
    private Float capitalSocial;

    @MappedFieldFileWithColumnCassandra(32)
    private String porteEmpresa;

    @MappedFieldFileWithColumnCassandra(33)
    private String opcaoPeloSimplesNacional;

    @MappedFieldFileWithColumnCassandra(34)
    private LocalDate dataOpcaoPeloSimplesNacional;

    @MappedFieldFileWithColumnCassandra(35)
    private LocalDate dataExclusaoSimplesNacional;

    @MappedFieldFileWithColumnCassandra(36)
    private String opcaoPeloMei;

    @MappedFieldFileWithColumnCassandra(37)
    private String situacaoEspecial;

    @MappedFieldFileWithColumnCassandra(38)
    private LocalDate dataSituacaoEspecial;

    @MappedFieldFileWithColumnCassandra(39)
    private LocalDateTime dataHoraInsercao;

}