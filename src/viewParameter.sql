/*
# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au
*/

CREATE VIEW viewParameter
AS
SELECT 
parameter.pulsar_id,parameter.parameter_id,parameter.citation_id as "parameter.citation_id",parameter.linkedSet_id,fitParameters.fitParameters_id,parameter.observingSystem_id,parameter.parameterType_id,ancillary.ancillary_id,linkedSet.citation_id AS "linkedSet.citation_id",fitParameters.citation_id AS "fitParameters.citation_id",tagToLinkedSet.tagToLinkedSet_id,tag.tag_id,derived.derived_id,derived.parameter_id AS "derived.parameter_id",derivedFromParameter.derivedFromParameter_id,derivedFromParameter.parameter_id AS "derivedFromParameter.parameter_id",
parameterType.label,parameter.value,parameter.uncertainty,parameter.referenceTime,parameter.timeDerivative,parameter.companionNumber,parameterType.unit,parameterType.description,parameterType.timingFlag,parameterType.dataType,observingSystem.systemLabel,observingSystem.centralFrequency,observingSystem.bandwidth,observingSystem.telescope,observingSystem.approximate,ancillary.value AS "ancillary.value",ancillary.description AS "ancillary.description",linkedSet.description AS "linkedSet.description",fitParameters.units,fitParameters.ephemeris,fitParameters.clock,tag.tagLabel,tag.tagString,derived.method AS "derived.method",derived.methodVersion AS "derived.methodVersion",citation.v1label AS "citation.v1label",citation.label AS "citation.label",citation.title AS "citation.title",citation.author AS "citation.author",citation.journal AS "citation.joural",citation.year AS "citation.year",citation.month AS "citation.month",citation.volume AS "citation.volume",citation.number AS "citation.number",citation.pages AS "citation.pages",citation.doi AS "citation.doi",citation.url AS "citation.url",
parameter.entryDate AS "parameter.entryDate",parameterType.entryDate AS "parameterType.entryDate",observingSystem.entryDate AS "observingSystem.entryDate",tag.entryDate AS "tag.entryDate"
FROM 
parameter 
LEFT JOIN parameterType ON parameterType.parameterType_id=parameter.parameterType_id 
LEFT JOIN observingSystem ON observingSystem.observingSystem_id=parameter.linkedSet_id
LEFT JOIN ancillary ON ancillary.parameter_id=parameter.parameter_id
LEFT JOIN linkedSet ON linkedSet.linkedSet_id=parameter.linkedSet_id
LEFT JOIN fitParameters ON fitParameters.fitParameters_id=parameter.fitParameters_id
LEFT JOIN tagToLinkedSet ON tagToLinkedSet.linkedSet_id=parameter.linkedSet_id
LEFT JOIN tag ON tag.tag_id=tagToLinkedSet.tag_id
LEFT JOIN derived ON derived.parameter_id=parameter.parameter_id
LEFT JOIN derivedFromParameter ON derivedFromParameter.parameter_id=parameter.parameter_id
LEFT JOIN citation ON citation.citation_id=parameter.citation_id
