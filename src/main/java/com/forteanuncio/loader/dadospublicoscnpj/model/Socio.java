package com.forteanuncio.loader.dadospublicoscnpj.model;

import java.io.Serializable;
import java.util.Date;
import java.util.UUID;

import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@Table
public class Socio implements Serializable{
    
    private static final long serialVersionUID = 911250549822685610L;

    @PrimaryKeyColumn(name = "id", ordinal = 0, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private UUID id;

    @PrimaryKeyColumn(name = "cnpj", ordinal = 1, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.ASCENDING)
    private Long cnpj;

    @PrimaryKeyColumn(name = "cpfCnpjSocio", ordinal = 2, type = PrimaryKeyType.PARTITIONED, ordering = Ordering.ASCENDING)
    private Long cpfCnpjSocio;

    @Column
    @Indexed("idxIdentificadorSocio")
    private Integer identificadorSocio;

    @Column
    @Indexed("idxNomeSocio")
    private String nomeSocio;

    @Column
    private String codigoQualificacaoSocio;

    @Column
    @Indexed("idxDataEntradaSociedade")
    private Date dataEntradaSociedade;

    @Column
    private String codigoPais;

    @Column
    private String nomePaisSocio;

    @Column
    @Indexed("idxCpfRepresentanteLegal")
    private Long cpfRepresentanteLegal;

    @Column
    private String nomeRepresentanteLegal;

    @Column
    private String codigoQualificacaoRepresentanteLegal;

}