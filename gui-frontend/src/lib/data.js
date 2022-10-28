import * as d3 from "d3";
import {writable} from "svelte/store";

function createDataset(testsCollection, ssrIDs, ssrIDsDB) {
    let d2 = [];
    let existingIDs = [];
    for (let test of testsCollection) {
        let ssr_id = test["_SSRID"]
        existingIDs.push(ssr_id)
        let individual_colors = [];
        for (const [key, value] of Object.entries(test)) {
            if (key !== "_SSRID") {
                individual_colors.push(value.match ? 2 : 1)
            }
        }
        let color = individual_colors.reduce((acc, val) => acc + val) / 4
        d2.push({"color": color, "ind_color": individual_colors, "ssr_id": ssr_id, "data": test})
    }

    for (let ssr_id of ssrIDsDB) {
        if (!existingIDs.includes(ssr_id)) {
            existingIDs.push(ssr_id)
            d2.push({"color": -1, "ind_color": [-1, -1, -1, -1], "ssr_id": ssr_id, "data": {"_SSRID": ssr_id}})
        }
    }

    for (let ssr_id of ssrIDs) {
        if (!existingIDs.includes(ssr_id)) {
            d2.push({"color": 0, "ind_color": [0, 0, 0, 0], "ssr_id": ssr_id, "data": {"_SSRID": ssr_id}})
        }
    }

    d2.sort((a, b) => d3.ascending(a.ssr_id, b.ssr_id))

    return d2
}

// from https://stackoverflow.com/a/71806500
function createStore() {
    let backend_ip = import.meta.env.VITE_BACKEND_URL
    let backend_port = import.meta.env.VITE_BACKEND_PORT
    const {subscribe, update, set} = writable([]);
    return {
        subscribe,
        async init() {
            let testsCollection;
            let ssrIDs;
            let ssrIDsDB;
            let data;
            let res1 = d3.json(`http://${backend_ip}:${backend_port}/tests`).then(data => {
                testsCollection = data;
            })
            let res2 = d3.json(`http://${backend_ip}:${backend_port}/ssr_ids`).then(data => {
                ssrIDs = data;
            })
            let res3 = d3.json(`http://${backend_ip}:${backend_port}/ssr_ids_db`).then(data => {
                ssrIDsDB = data;
            })
            await Promise.all([res1, res2, res3]).then(results => {
                data = createDataset(testsCollection, ssrIDs, ssrIDsDB)
            })
            set(data)
        }
    }
}

export default createStore();