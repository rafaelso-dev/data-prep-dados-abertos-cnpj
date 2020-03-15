package com.forteanuncio.prep.dadospublicoscnpj.models;

import java.io.Serializable;
import java.time.LocalDateTime;

import com.datastax.driver.core.LocalDate;

public class Empresa implements Serializable{
    
    private static final long serialVersionUID = -6634755854220896813L;

    private Long cnpj;
    
    private Integer matriz;

    private String razaoSocial;

    private String nomeFantasia;
    
    private Integer situacaoCadastral;
    
    private LocalDate dataSituacaoCadastral;
    
    private Integer codigoMotivoSitualCadastral;
    
    private String nomeCidadeExterior;
    
    private String codigoPais;
    
    private String nomePais;
    
    private Integer codigoNaturezaJuridica;
    
    private LocalDate dataInicioAtividade;
    
    private Integer cnaeFiscal;
    
    private String descLogradouro;
    
    private String logradouro;

    private String numero;

    private String complemento;

    private String bairro;

    private Integer cep;

    private String uf;

    private Integer codigoMunicipio;

    private String municipio;

    private Integer ddd1;

    private Integer telefone1;
    
    private Integer ddd2;
    
    private Integer telefone2;
    
    private Integer dddFax;
    
    private Integer numFax;

    private String email;
    
    private Integer qualificacaoResponsavel;

    private Float capitalSocial;

    private String porteEmpresa;

    private String opcaoPeloSimplesNacional;

    private LocalDate dataOpcaoPeloSimplesNacional;

    private LocalDate dataExclusaoSimplesNacional;

    private String opcaoPeloMei;

    private String situacaoEspecial;

    private LocalDate dataSituacaoEspecial;

    private LocalDateTime dataHoraInsercao;

}