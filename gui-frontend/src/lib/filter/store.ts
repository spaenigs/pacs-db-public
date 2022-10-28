import { writable } from 'svelte/store';

type LogicalOp = "and" | "or"

type Op = "equals" | "less than" | "less than or equals" | 
          "greater than" | "greater than or equals"

type Collection = "Studies" | "Series" | "Instances" | 
                  "Swiss Stroke Registry"

type QueryResult = {
    _SSRID: string
}

type AcquisitionState = "Internal" | "External"

type StudyResult = QueryResult & {
    _StudyTimeExact: Date,
    AccessionNumber: string,
    ModalitiesInStudy: string[],
    StudyDescription: string, 
    PatientName: string, 
    PatientID: string,
    _AcquisitionState: AcquisitionState
    _AcquisitionNumber: number, 
}

type SeriesResult = StudyResult & {
    _SeriesTimeExact: Date,
    _SequenceType: string
}

type Modality = "MR" | "CT"

type InstanceResult = SeriesResult & {
    _AcqusitionTimeExact: Date,
    BodyPartExamined: string, 
    ImageType: string, 
    InstitutionAddress: string,
    InstitutionName: string,
    Modality: Modality,
    PatientAge: number
    PatientBirthDate: Date
    PatientSex: string,
    ProtocolName: string, 
    StationName: string,
    SequenceName: string,
    SliceLocation?: number, 
    SliceThickness?: number,
    PixelSpacing?: [number, number]
}

type Tab = {
    header: string, 
    idx: number, 
    collections: Collection[], 
    selectedFields: string[],
    selectedOps: Op[],
    selectedVals: string[], 
    numberOfRows: number, 
    logical_op: LogicalOp, 
    queryRes?: QueryResult
}

let lst: Tab[] = []

export const tabStore = writable([]);
export const activeIdx = writable(0)
export const joinRes = writable(null)

const study_fields = [
    'AccessionNumber', 'ModalitiesInStudy', 'StudyDescription', 'PatientName', 'PatientID',
    '_SSRID', '_StudyTimeExact', '_AcquisitionNumber', '_AcquisitionState'
].sort()

const series_fields = [
    'AccessionNumber', 'ModalitiesInStudy', 'StudyDescription', 'SeriesDescription', 'PatientName',
    'PatientID', '_StudyTimeExact', '_SeriesTimeExact', '_SSRID', '_SequenceType'
].sort()

const instances_fields = [
    "AccessionNumber", "BodyPartExamined", "ImageType", "InstitutionAddress",
    "InstitutionName", "Manufacturer", "ManufacturerModelName", "Modality", "PatientAge",
    "PatientBirthDate", "PatientID", "PatientName", "PatientPosition", "PatientSex", "PixelSpacing",
    "ProtocolName", "SliceLocation", "SliceThickness", "SmallestImagePixelValue", "SoftwareVersions",
    "StationName", "WindowCenter", "_AcquisitionTimeExact", "_SSRID", "_SequenceType", "_SeriesTimeExact",
    "_StudyTimeExact", "SequenceName"
].sort()

const ssr_fields = [].sort()

export function getFields(collection) {
    switch (collection) {
        case "studies":
            return study_fields
        case "series":
            return series_fields
        case "instances":
            return instances_fields
        case "swiss_stroke_registry":
            return ssr_fields
        default:
            return [];
    }
}