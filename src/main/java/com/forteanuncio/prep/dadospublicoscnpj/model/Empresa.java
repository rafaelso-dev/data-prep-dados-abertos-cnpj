package com.forteanuncio.prep.dadospublicoscnpj.model;

import java.io.Serializable;
import java.time.LocalDateTime;

import com.datastax.driver.core.LocalDate;
import com.forteanuncio.prep.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@Table
@ToString
public class Empresa implements Serializable{
    
    private static final long serialVersionUID = -6634755854220896813L;

    @PrimaryKeyColumn(name = "cnpj", ordinal = 1, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.DESCENDING)
    @MappedFieldFileWithColumnCassandra(1)
    private Long cnpj;
    
    @Column
    @MappedFieldFileWithColumnCassandra(2)
    private Integer matriz;

    @MappedFieldFileWithColumnCassandra(3)
    @PrimaryKeyColumn(name = "razaoSocial", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
    private String razaoSocial;

    @MappedFieldFileWithColumnCassandra(39)
    @PrimaryKeyColumn(name = "dataHoraInsercao", ordinal = 3, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private LocalDateTime dataHoraInsercao;

    
    @Indexed("idxNomeFantasia")
    @MappedFieldFileWithColumnCassandra(4)
    private String nomeFantasia;
    
    @Column
    @Indexed("situacaoCadastral")
    @MappedFieldFileWithColumnCassandra(5)
    private Integer situacaoCadastral;
    
    @Column
    @Indexed("idxDataSituacaoCadastral")
    @MappedFieldFileWithColumnCassandra(6)
    private LocalDate dataSituacaoCadastral;
    
    @Column
    @Indexed("idxCodigoMotivoSitualCadastral")
    @MappedFieldFileWithColumnCassandra(7)
    private Integer codigoMotivoSitualCadastral;
    
    @Column
    @MappedFieldFileWithColumnCassandra(8)
    private String nomeCidadeExterior;
    
    @Column
    @MappedFieldFileWithColumnCassandra(9)
    private String codigoPais;
    
    @Column
    @MappedFieldFileWithColumnCassandra(10)
    private String nomePais;
    
    @Column
    @MappedFieldFileWithColumnCassandra(11)
    private Integer codigoNaturezaJuridica;
    
    @Column
    @MappedFieldFileWithColumnCassandra(12)
    private LocalDate dataInicioAtividade;
    
    @Column
    @Indexed("idxCnaeFiscal")
    @MappedFieldFileWithColumnCassandra(13)
    private Integer cnaeFiscal;
    
    @Column
    @MappedFieldFileWithColumnCassandra(14)
    private String descLogradouro;
    
    @Column
    @Indexed("idxLogradouro")
    @MappedFieldFileWithColumnCassandra(15)
    private String logradouro;

    @Column
    @MappedFieldFileWithColumnCassandra(16)
    private String numero;

    @Column
    @MappedFieldFileWithColumnCassandra(17)
    private String complemento;

    @Column
    @Indexed("idxBairro")
    @MappedFieldFileWithColumnCassandra(18)
    private String bairro;

    @Column
    @Indexed("idxCep")
    @MappedFieldFileWithColumnCassandra(19)
    private Integer cep;

    @Column
    @Indexed("idxUf")
    @MappedFieldFileWithColumnCassandra(20)
    private String uf;

    @Column
    @MappedFieldFileWithColumnCassandra(21)
    private Integer codigoMunicipio;

    @Column
    @Indexed("idxMunicipio")
    @MappedFieldFileWithColumnCassandra(22)
    private String municipio;

    @Column
    @Indexed("idxDDD1")
    @MappedFieldFileWithColumnCassandra(23)
    private Integer ddd1;

    @Column
    @Indexed("idxTelefone1")
    @MappedFieldFileWithColumnCassandra(24)
    private Integer telefone1;
    
    @Column
    @Indexed("idxDDD2")
    @MappedFieldFileWithColumnCassandra(25)
    private Integer ddd2;
    
    @Column
    @MappedFieldFileWithColumnCassandra(26)
    @Indexed("idxTelefone2")
    private Integer telefone2;
    
    @Column
    @Indexed("idxDDDFax")
    @MappedFieldFileWithColumnCassandra(27)
    private Integer dddFax;
    
    @Column
    @Indexed("idxNumFax")
    @MappedFieldFileWithColumnCassandra(28)
    private Integer numFax;

    @Column
    @Indexed("idxEmail")
    @MappedFieldFileWithColumnCassandra(29)
    private String email;
    
    @Column
    @MappedFieldFileWithColumnCassandra(30)
    private Integer qualificacaoResponsavel;

    @Column
    @Indexed("idxCapitalSocial")
    @MappedFieldFileWithColumnCassandra(31)
    private Float capitalSocial;

    @Column
    @Indexed("idxPorteEmpresa")
    @MappedFieldFileWithColumnCassandra(32)
    private String porteEmpresa;

    @Column
    @Indexed("idxSimplesNacional")
    @MappedFieldFileWithColumnCassandra(33)
    private String opcaoPeloSimplesNacional;

    @Column
    @MappedFieldFileWithColumnCassandra(34)
    private LocalDate dataOpcaoPeloSimplesNacional;

    @Column
    @MappedFieldFileWithColumnCassandra(35)
    private LocalDate dataExclusaoSimplesNacional;

    @Column
    @Indexed("idxMei")
    @MappedFieldFileWithColumnCassandra(36)
    private String opcaoPeloMei;

    @Column
    @MappedFieldFileWithColumnCassandra(37)
    private String situacaoEspecial;

    @Column
    @MappedFieldFileWithColumnCassandra(38)
    private LocalDate dataSituacaoEspecial;

}