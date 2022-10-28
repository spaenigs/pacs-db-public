import * as d3 from "d3";
import {DateTime, Settings} from "luxon";
import { bannerText, showBanner } from "./store.js";

Settings.throwOnInvalid = true;

export async function post_query(data, collection) {
    let backend_ip = import.meta.env.VITE_BACKEND_URL
    let backend_port = import.meta.env.VITE_BACKEND_PORT
    let dataReceived;
    // @ts-ignore
    await fetch(`http://${backend_ip}:${backend_port}/${collection}`, {
        method: "post",
        headers: { "Content-Type": "application/json" },
        body: data 
    }).then(resp => {
        if (resp.status === 200) {
            return resp.json()
        } else {
            console.log("Status: " + resp.status)
            return Promise.reject("server")
        }
    }).then(dataJson => {
        dataReceived = dataJson
    }).catch(err => {
        if (err === "server") return
        console.log(err)
    })
    return dataReceived
}

function getOp(op) {
    switch (op) {
        case "and":
            return "$and"
        case "or":
            return "$or"
        case "equals":
            return "$eq"
        case "less than":
            return "$lt"
        case "less than or equals":
            return "$lte"
        case "greater than":
            return "$gt"
        case "greater than or equals":
            return "$gte"
    }
}

export function prepare_query(numberOfRows, selectedFields, selectedOps, selectedVals, logical_op) {
    let inner = [];
    for (let idx in [...Array(numberOfRows).keys()]) {
        let obj = {}
        let cond = {}
        let parsed_val
        if (selectedFields[idx] === "_AcquisitionNumber") {
            parsed_val = parseInt(selectedVals[idx])
            parsed_val = isNaN(parsed_val) ? selectedVals[idx] : parsed_val
        } else if (selectedFields[idx].includes("Time")) {
            // store the path to date here to allow date parsing on the server
            // which is required for mongo db to compare dates
            let date = DateTime.fromISO("1000");
            try {
                date = DateTime.fromISO(selectedVals[idx])
            } catch (e) {
                bannerText.update(() => "Invalid date: " + selectedVals[idx] + "!")
                showBanner.update(() => true)
            }
            parsed_val = {
                date: date,
                path: getOp(logical_op) + ":" + idx + ":" + selectedFields[idx] + ":" + getOp(selectedOps[idx])
            }
        } else {
            parsed_val = selectedVals[idx]
        }
        cond[getOp(selectedOps[idx])] = parsed_val
        obj[selectedFields[idx]] = cond  
        inner.push(obj)      
    }
    let query = {}
    query[getOp(logical_op)] = inner
    return query
}

export const parseDate = (d) => d3.timeParse("%Y-%m-%d %H:%M:%S")(d)

export function to_csv(qres) {
    let headerPos = {}
    let cnt = 0;
    for (let row of qres) {
        for (let k of Object.keys(row)) {
            if (!(k in headerPos)) {
                headerPos[k] = cnt
                cnt += 1
            }
        }
    }
    let data = "data:application/vnd.ms-excel;charset=utf-8,"
    data += Object.keys(headerPos).join("\t") + "\n"
    for (let row of qres) {
        for (let k of Object.keys(headerPos)) {
            if (Object.keys(row).includes(k)) {
                data += row[k] + "\t"
            } else {
                data += "\t"
            }
        }
        data += "\n"
    }
    return data
}
