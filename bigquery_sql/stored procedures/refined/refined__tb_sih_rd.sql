CREATE OR REPLACE PROCEDURE `datasus-prod.refined.tb_sih_rd`()
BEGIN

  CREATE OR REPLACE TABLE `datasus-prod.refined.tb_sih_rd` AS (
    SELECT
      sih.municipality_code                                                      AS municipality_code,
      aM.NOME                                                                    AS municipality,
      sih.date                                                                   AS date,
      sih.year                                                                   AS year,
      sih.month                                                                  AS month,
      sih.specialty_bed                                                          AS specialty_bed_code,
      CASE
        WHEN sih.specialty_bed = '01' THEN 'Cirurgia'
        WHEN sih.specialty_bed = '02' THEN 'Obstetrícia'
        WHEN sih.specialty_bed = '03' THEN 'Clínica Médica'
        WHEN sih.specialty_bed = '04' THEN 'Pacientes sob Cuidados Prolongados (Crônicos)'
        WHEN sih.specialty_bed = '05' THEN 'Psiquiatria'
        WHEN sih.specialty_bed = '06' THEN 'Pneumologia Sanitária (Tisiologia)'
        WHEN sih.specialty_bed = '07' THEN 'Pediatria'
        WHEN sih.specialty_bed = '08' THEN 'Reabilitação'
        WHEN sih.specialty_bed = '09' THEN 'Tratamento realizado em Hospital - Dia'
        WHEN sih.specialty_bed = '87' THEN 'Saúde Mental - Clínico'
        WHEN sih.specialty_bed = '14' THEN 'Saúde Mental - Dia'
        WHEN sih.specialty_bed = '10' THEN 'AIDS - Dia'
        WHEN sih.specialty_bed = '12' THEN 'Intercorrência Pós-Transplante - Dia'
        ELSE sih.specialty_bed 
      END                                                                        AS specialty_bed,
      sih.aih                                                                    AS aih,
      sih.type_aih                                                               AS type_aih_code,
      CASE
        WHEN sih.type_aih = '1' THEN 'AIH Normal'
        WHEN sih.type_aih = '5' THEN 'AIH de Longa Permanência e FTP'
        ELSE sih.type_aih
      END                                                                        AS type_aih,
      sih.patient_municipality                                                   AS patient_municipality_code,
      aMp.NOME                                                                   AS patient_municipality,
      sih.gender                                                                 AS gender_code,
      CASE
        WHEN sih.gender = '0' THEN 'Ignorado'
        WHEN sih.gender = '3' THEN 'Feminino'
        WHEN sih.gender = '1' THEN 'Masculino'
        ELSE sih.gender
      END                                                                        AS gender,
      sih.type_uti                                                               AS type_uti_code,           
      CASE
        WHEN sih.type_uti = '00' THEN 'UTI tipo I, Leito sem especialidade ou não utilizou UTI'
        WHEN sih.type_uti = '75' THEN 'UTI Adulto - tipo II'
        WHEN sih.type_uti = '81' THEN 'UTI Neonatal - tipo II'
        WHEN sih.type_uti = '78' THEN 'UTI Infantil - tipo II'
        WHEN sih.type_uti = '85' THEN 'UTI Coronariana tipo II - UCO tipo II'
        WHEN sih.type_uti = '76' THEN 'UTI Adulto - tipo III'
        WHEN sih.type_uti = '79' THEN 'UTI Infantil - tipo III'
        WHEN sih.type_uti = '86' THEN 'UTI Coronariana tipo III - UCO tipo III'
        WHEN sih.type_uti = '01' THEN 'Utilizou mais de um tipo de UTI'
        WHEN sih.type_uti = '83' THEN 'UTI de Queimados'
        WHEN sih.type_uti = '82' THEN 'UTI Neonatal - tipo III'
        WHEN sih.type_uti = '99' THEN 'UTI de Doador'
        ELSE sih.type_uti 
      END                                                                        AS type_uti,
      sih.days_hospitalized                                                      AS days_hospitalized,
      sih.procedure                                                              AS procedure_code,
      aP.PROCEDIMENTO                                                            AS procedure,
      sih.total_value                                                            AS total_value,
      sih.death                                                                  AS death,
      sih.risk_pregnant                                                          AS risk_pregnant,
      sih.race_color                                                             AS race_color_code,
      CASE
        WHEN sih.race_color = '01' THEN 'Branca'
        WHEN sih.race_color = '02' THEN 'Preta'
        WHEN sih.race_color = '03' THEN 'Parda'
        WHEN sih.race_color = '04' THEN 'Amarela'
        WHEN sih.race_color = '05' THEN 'Indígena'
        WHEN sih.race_color = '99' THEN 'Sem Informações'
        ELSE sih.race_color
      END                                                                        AS race_color,
      sih.birth_date                                                             AS birth_date,
      sih.age                                                                    AS age
    FROM
      `datasus-prod.trusted.tb_sih_rd` AS sih

    LEFT JOIN
      `datasus-prod.lookup.tb_lookup_municipality` AS aM
    ON
      sih.municipality_code = aM.COD

    LEFT JOIN
      `datasus-prod.lookup.tb_lookup_municipality` AS aMp
    ON
      sih.patient_municipality = aMp.COD

    LEFT JOIN
      `datasus-prod.lookup.tb_lookup_hospitalization_procedure` AS aP
    ON
      SUBSTR(sih.procedure, 2) = aP.COD
  );
  
END;