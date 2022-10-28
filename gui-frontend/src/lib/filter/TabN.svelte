<svelte:head>
  <link href="./src/lib/filter/tabulator.min.css" rel="stylesheet">
</svelte:head>

<script lang="ts">
    import Button from '@smui/button';
    import { activeIdx, getFields, tabStore } from "./store.js"
    import Textfield from '@smui/textfield';
    import LayoutGrid, {Cell} from "@smui/layout-grid"
    import Select, {Option} from "@smui/select";
    import { post_query, prepare_query, to_csv } from "../utils";
    import { TabulatorFull as Tabulator } from "tabulator-tables";

    /**
     * Represents one tab in the Filter view. Allows querying the database and representing the results in tables.
     */

    function getQuery() {
        let lastIdx = $tabStore[$activeIdx].collections.length - 1;
        let collection = $tabStore[$activeIdx].collections[lastIdx].toLowerCase()
        if (collection === "join") {
            let leftCollection = $tabStore[$activeIdx].selectedFields[lastIdx]
            let rightCollection = $tabStore[$activeIdx].selectedOps[lastIdx]
            let leftTableIdx = $tabStore.filter(t => t.header === leftCollection).map(t => t.idx)[0]
            let rightTableIdx = $tabStore.filter(t => t.header === rightCollection).map(t => t.idx)[0]
            let local_query = prepare_query(
                $tabStore[leftTableIdx].numberOfRows, $tabStore[leftTableIdx].selectedFields,
                $tabStore[leftTableIdx].selectedOps, $tabStore[leftTableIdx].selectedVals,
                $tabStore[leftTableIdx].logical_op
            )
            let foreign_query = prepare_query(
                $tabStore[rightTableIdx].numberOfRows, $tabStore[rightTableIdx].selectedFields,
                $tabStore[rightTableIdx].selectedOps, $tabStore[rightTableIdx].selectedVals,
                $tabStore[rightTableIdx].logical_op
            ) 
            let query = {
                left_collection: $tabStore[leftTableIdx].collections[0].toLowerCase(),
                right_collection: $tabStore[rightTableIdx].collections[0].toLowerCase(),
                left_query: {query: local_query, start: 0, end: 0},
                right_query: {query: foreign_query, start: 0, end: 0},
                on: $tabStore[$activeIdx].selectedVals[lastIdx]
            }
            return [query, collection]
        } else {
            let query = {
                query: prepare_query(
                    $tabStore[$activeIdx].numberOfRows, $tabStore[$activeIdx].selectedFields, 
                    $tabStore[$activeIdx].selectedOps, $tabStore[$activeIdx].selectedVals, 
                    $tabStore[$activeIdx].logical_op)
            }
            return [query, collection]
        }
    }

    async function initTable() {
        tableLoaded = false
        let [query, collection] = getQuery();
        setTable(query, collection)
    }

    function resetQueryResult() {
        if ($tabStore[$activeIdx].collections.includes("Join")) {
            $tabStore[$activeIdx].selectedFields = [""]
            $tabStore[$activeIdx].selectedOps = [""]
            $tabStore[$activeIdx].selectedVals = [""]
            $tabStore[$activeIdx].collections = ["Join"]
            $tabStore[$activeIdx].logical_op = "join"
            $tabStore[$activeIdx].numberOfRows = 1
        } else {
            let lastIdx = $tabStore[$activeIdx].selectedFields.length - 1
            $tabStore[$activeIdx].selectedFields[lastIdx] = "_SSRID"
            $tabStore[$activeIdx].selectedVals[lastIdx] = ""
            $tabStore[$activeIdx].selectedOps[lastIdx] = "equals"
            $tabStore[$activeIdx].logical_op = "and"
        }
        $tabStore[$activeIdx].queryRes = []
    }

    function getTabNames() {
        let allNames = $tabStore.map(t => t.header)
        return allNames.filter(h => {
            switch (h) {
                case "join":
                    return false
                case "add":
                    return false
                case $tabStore[$activeIdx].header:
                    return false
                default:
                    return true;
            }
        })
    }

    let tableComponent;
    let  tableLoaded = false;

    function setTable(query, collection) {
        let backend_ip = import.meta.env.VITE_BACKEND_URL
        let backend_port = import.meta.env.VITE_BACKEND_PORT
        let table = new Tabulator(tableComponent, {
            autoColumns:true,
            layout: "fitData",
            ajaxURL: `http://${backend_ip}:${backend_port}/${collection}`,
            ajaxConfig: "POST",
            pagination: true, 
            paginationMode: "remote",
            paginationSize: 30,
            ajaxRequestFunc: function(url, config, params) {
                query["start"] = params.page
                query["end"] = params.size
                return fetch(url, {
                    method: "post",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(query)
                }).then(resp => {
                    if (resp.status === 200) {
                        return resp.json()
                    } else {
                        return Promise.reject("server")
                    }
                })
            }
        })
        table.on("dataLoaded", function(data){
            tableLoaded = true
        });       
    }

    let logical_ops = ["and", "or"]

    let collections = ["Studies", "Series", "Instances", "Swiss_Stroke_Registry", "Join"];

    let operators = [
        "equals", "less than", "less than or equals", 
        "greater than", "greater than or equals"
    ]

    let joinKeys = ["AccessionNumber", "_SSRID"]

</script>

<LayoutGrid>
    <Cell span={12}>
        <Textfield label="Tab Name" bind:value={$tabStore[$activeIdx].header}></Textfield>
        <br><br>
        {#each [...Array($tabStore[$activeIdx].numberOfRows).keys()] as idx, i}                
            <Select label="Query Level" variant="outlined"
                    on:MDCSelect:change={() => resetQueryResult()}
                    bind:value={$tabStore[$activeIdx].collections[idx]}>
                {#each collections as l, i}
                    <Option value={l}>{l}</Option>
                {/each}
            </Select>
            {#if $tabStore[$activeIdx].collections[idx] === "Join"}
                <Select label="Left" variant="outlined" 
                        bind:value={$tabStore[$activeIdx].selectedFields[idx]}>
                    {#each getTabNames() as f}
                        <Option value={f}>{f}</Option>
                    {/each}
                </Select> 
                <Select label="Right" variant="outlined"
                    bind:value={$tabStore[$activeIdx].selectedOps[idx]} >
                    {#each getTabNames() as o}
                        <Option value={o}>{o}</Option>
                    {/each}
                </Select>
                <Select label="On" variant="outlined"
                    on:MDCSelect:change={() => 1}
                    bind:value={$tabStore[$activeIdx].selectedVals[idx]}>
                {#each joinKeys as l, i}
                    <Option value={l}>{l}</Option>
                {/each}
                </Select>
                <br><br>
            {:else}
                <Select label="Field" variant="outlined" bind:value={$tabStore[$activeIdx].selectedFields[idx]}>
                    {#each getFields($tabStore[$activeIdx].collections[idx].toLowerCase()) as f}
                        <Option value={f}>{f}</Option>
                    {/each}
                </Select> 
                <Select label="Operator" variant="outlined"
                        bind:value={$tabStore[$activeIdx].selectedOps[idx]} >
                    {#each operators as o}
                        <Option value={o}>{o}</Option>
                    {/each}
                </Select>
                <Textfield variant="outlined" bind:value={$tabStore[$activeIdx].selectedVals[idx]} label="Value"></Textfield>
                {#if idx === $tabStore[$activeIdx].numberOfRows - 1}
                    <Button on:click={() => {
                        let lastIdx = $tabStore[$activeIdx].selectedFields.length - 1
                        $tabStore[$activeIdx].selectedFields.push($tabStore[$activeIdx].selectedFields[lastIdx])
                        $tabStore[$activeIdx].selectedOps.push($tabStore[$activeIdx].selectedOps[lastIdx])                    
                        $tabStore[$activeIdx].selectedVals.push($tabStore[$activeIdx].selectedVals[lastIdx])                    
                        $tabStore[$activeIdx].collections.push("Studies")
                        $tabStore[$activeIdx].numberOfRows += 1
                    }} variant="raised" style="height: 30px; width: 30px">
                        +
                    </Button>
                {:else if idx === $tabStore[$activeIdx].numberOfRows - 2}
                    <Button on:click={() => {
                        $tabStore[$activeIdx].selectedFields.pop()
                        $tabStore[$activeIdx].selectedOps.pop()
                        $tabStore[$activeIdx].selectedVals.pop()
                        $tabStore[$activeIdx].collections.pop()
                        $tabStore[$activeIdx].numberOfRows -= 1
                    }} variant="outlined">
                        -
                    </Button>
                {:else}
                    <Button disabled></Button>
                {/if}
                <br><br>
            {/if}
        {/each}
        {#if $tabStore[$activeIdx].numberOfRows > 1}
            <br>
            <Select label="Select Operator" variant="outlined"
                    bind:value={$tabStore[$activeIdx].logical_op} >
                {#each logical_ops as op}
                    <Option value={op}>{op}</Option>
                {/each}
            </Select>
            <br><br>
        {/if}
        <Button on:click={async () => initTable()}>Submit</Button>
    </Cell>
    <Cell span={12}>
        <div bind:this={tableComponent} style="width: 100%"></div>
    </Cell>
    {#if tableLoaded}
        <Cell span={12}>
            <Button on:click={async () => {
                let [query, collection] = getQuery();
                query["start"] = 0
                query["end"] = 0
                let queryRes = await post_query(JSON.stringify(query), collection);
                let queryRes_ = queryRes ?? {data: [{}]}               
                let csvContent = to_csv(queryRes_.data)               
                var encodedUri = encodeURI(csvContent);
                window.open(encodedUri);
            }}>Download</Button>
        </Cell>
    {/if}
</LayoutGrid>