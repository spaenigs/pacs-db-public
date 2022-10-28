import { writable } from 'svelte/store';
import { parseDate, post_query } from "../utils.js";
import * as d3 from "d3";

export const rects = writable(null);

export const testSelection = writable({});

export function calcData(data) {
    let res = [];
    for (let [key, value] of Object.entries(data)) {
        if (key !== "_SSRID" && key !== "door_to_image_time") {
            for (let [ki, vi] of Object.entries(value)) {
                if (ki !== "match") {
                    res.push({
                        "src": ki,
                        "type": key.includes("external") ? "external_imaging_1" : key,
                        "date": parseDate(value[ki]["_AcquisitionTimeExact"]),
                        "ssr_id": data["_SSRID"],
                        "acc_nr": vi["AccessionNumber"],
                        "from": "manual_vs_auto"
                    })
                }
            }
        }
    }
    res = d3.filter(res, (d) => d.date !== null)
    res = res.sort((a, b) => d3.ascending(a.date, b.date))
    for (let idx in res) {
        if (res[idx].date !== null) {
            res[idx]["minute"] = d3.timeMinute.count(res[0].date, res[idx].date)
        }
    }
    return res
}

function parseModality(modalitiesInStudy) {
    if (modalitiesInStudy.includes("MR")) {
        return "MR"
    } else if (modalitiesInStudy.includes("CT")) {
        return "CT"
    } else {
        return  "XA"
    }
}

export function getStudyInfo(data) {
    for (let idx in data) {
        let acc_nr = data[idx].acc_nr
        let query = JSON.stringify({query: {AccessionNumber: acc_nr}, start: 0, end:0})
        post_query(query, "studies").then(d => {
            let d_ = d ?? {data: [{}]}
            let qres = d_.data
            data[idx].Modality = parseModality(qres[0].ModalitiesInStudy)
            data[idx].PatientID = qres[0].PatientID
        })
    }
    return data
}

function getType(state, nr) {
    switch (state) {
        case "External":
            return `external_imaging_${nr}`
        case "Internal":
            return `internal_imaging_${nr}`
    }
}

export async function getAllStudies(ssr_id) {
    let res = [];
    let query = JSON.stringify({query: {_SSRID: ssr_id}, start: 0, end:0})
    await post_query(query, "studies").then(d => {
        let d_ = d ?? {data: [{}]}
        for (let r of d_.data) {
            let t = getType(r._AcquisitionState, r._AcquisitionNumber);
            if (t !== undefined) {
                res.push({date: parseDate(r._StudyTimeExact.replace("T", " ")),
                    acc_nr: r.AccessionNumber, StudyDescription: r.StudyDescription,
                    PatientID: r.PatientID, Modality: parseModality(r.ModalitiesInStudy),
                    _AcquisitionState: r._AcquisitionState, _AcquisitionNumber: r._AcquisitionNumber,
                    src: "db", type: t, cat: t, "ssr_id": r._SSRID, "from": "all_studies"
                })
            }
        }
    })
    res = res.sort((a,b) => d3.ascending( a.date, b.date))
    return res
}

export const allStudies = writable(null);