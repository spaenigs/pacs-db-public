<script lang="ts">
    import * as d3 from 'd3';
    import { testSelection, allStudies } from "./store";

    /**
     * Entrypoint of the Times module. Shows study information stored in the database and the manual curated table.
     */

    function getTime(tInMin) {
        if (tInMin <= 59) {
            return tInMin + "m"
        } else if (tInMin <= 1439) {
            return Math.round(tInMin / 60) + "h"
        } else {
            return Math.round(tInMin / 60 / 24) + "d"
        }
    }

    let img_types = ["external_imaging_1", "internal_imaging_1", "internal_imaging_2", "more studies..."]
    let data;
    let showChart = false
    let tooltip_x;
    let tooltip_y;
    let tooltip_ssr_id;
    let tooltip_acc_nr;
    let tooltip_type;
    let tooltip_src;
    let tooltip_date;
    let tooltip_modality;
    let tooltip_pid;
    let tooltip_from;
    let isHovered = false;

    let x1;
    let x2;
    let y;

    let margin = {top: 20, right: 20, bottom: 20, left: 20}
    let width = 450 - margin.left - margin.right
    let height = 200 - margin.top - margin.bottom

    $: if (Object.keys($testSelection).length === 0) {
        showChart = false
    } else if (Object.keys($testSelection).length > 0) {
        data = $testSelection
        showChart = true
        let res = {}
        for (let d of data) {
            if (Object.keys(res).includes(d.type)) {
                res[d.type].push(d.minute)
            } else {
                res[d.type] = [d.minute]
            }
        }
        let groups_obj = {};
        for (let key of Object.keys(res)) {
            if (res[key].length > 1) {
                let [k1, k2] = res[key].sort()
                if (k2 - k1 < 10) {
                    groups_obj[k1] = `${k1}, ${k2}`
                    groups_obj[k2] = `${k1}, ${k2}`
                } else {
                    groups_obj[k1] = k1.toString()
                    groups_obj[k2] = k2.toString()
                }
            } else {
                groups_obj[res[key][0]] = res[key][0].toString()
            }
        }
        for (let d of data) {
            d.cat = groups_obj[d.minute]
        }
        let numberOfRects = [...new Set(data.map(d => d.cat))].length
        width = numberOfRects * 80
        x1 = d3.scaleBand()
            .range([0, width])
            .domain(data.map(d => d.cat))
            .padding(0.05);
        x2 = d3.scaleBand()
            .range([0, width + 200])
            .domain($allStudies.map(d => d.cat))
            .padding(0.05);
        y = d3.scaleBand()
            .range([height, 0])
            .domain(["db", "excel"])
            .padding(0.05)
    }

    let colorScale = d3.scaleOrdinal()
        .range(["#cbc9e2", "#756bb1", "#54278f"])
        .domain(["external_imaging_1", "internal_imaging_1", "internal_imaging_2"])
        .unknown("black")
   
</script>

{#if showChart}
    <div style="background-color: white; border: 2px solid black; border-radius: 5px" on:click={() => showChart = false}>
        <p>(Click to close)</p>
        <svg height="30px" width="250px">
            <text x="20" y="20" font-weight="bold">Manual vs. Automatic</text>
        </svg>
        <svg width="750px" height="250px">
            <g >
                <line
                    x1="{margin.left+margin.right}"
                    x2="{width+margin.left+margin.right}"
                    y1="{height+5}"
                    y2="{height+5}"
                    stroke="black">
                </line>
                <polygon
                    points="{width+margin.left+margin.right+15},{height+5}
                            {width+margin.left+margin.right},{height+10}
                            {width+margin.left+margin.right},{height}"/>
                <text x="{width+margin.left+margin.right+20}" y="{height+8}" font-size="11px">time</text>
            </g>
            <g transform="translate(0, {margin.top + margin.bottom})" style="font-size: 15;">
                {#each ["excel", "db"] as tp}
                    <g transform="translate(0, {y(tp)})">
                        <text y="2">{tp}</text>>
                    </g>
                {/each}
            </g>
            {#each data as d}
                <rect       
                    x="{x1(d.cat) + 40}"
                    y="{y(d.src)}"
                    rx="4"
                    ry="4"
                    opacity="0.8"
                    width="{x1.bandwidth()}"
                    height="{y.bandwidth()}"
                    fill="{colorScale(d.type)}"
                    stroke="{colorScale(d.type)}"
                    stroke-width="2px"
                    on:mouseover={(e) => {
                            isHovered = true
                            tooltip_x = e.pageX + 5;
                            tooltip_y = e.pageY + 5;
                            tooltip_ssr_id = d.ssr_id;
                            tooltip_acc_nr = d.acc_nr;
                            tooltip_type = d.type;
                            tooltip_src = d.src;
                            tooltip_date = d.date;
                            tooltip_modality = d.Modality
                            tooltip_pid = d.PatientID
                            tooltip_from = d.from
                            d3.select(e.currentTarget).attr("stroke", "black")
                        }}
                    on:mousemove={(e) => {
                            tooltip_x = e.pageX + 5;
                            tooltip_y = e.pageY + 5;
                        }}
                    on:mouseleave={(e) => {
                            isHovered = false
                            d3.select(e.currentTarget).attr("stroke", colorScale(d.type))
                        }}
                >
                </rect>
            {/each}
            {#each data as d}
                <text
                    x="{x1(d.cat) + 75}"
                    y="{y(d.src) + 45}"
                    text-anchor="middle"
                    fill="white"
                >{getTime(d.minute)}</text>
            {/each}
        </svg>
        <svg height="30px" width="250px">
            <text x="20" y="20" font-weight="bold">All Studies</text>
        </svg>
        <svg width="750px" height="250px">
            <g >
                <line
                        x1="{margin.left+margin.right}"
                        x2="{width+margin.left+margin.right+200}"
                        y1="{height+5}"
                        y2="{height+5}"
                        stroke="black">
                </line>
                <polygon
                        points="{width+margin.left+margin.right+15+200},{height+5}
                            {width+margin.left+margin.right+200},{height+10}
                            {width+margin.left+margin.right+200},{height}"/>
                <text x="{width+margin.left+margin.right+20+200}" y="{height+8}" font-size="11px">time</text>
            </g>
            <g transform="translate(0, {margin.top + margin.bottom})" style="font-size: 15;">
                {#each ["db"] as tp}
                    <g transform="translate(0, {y(tp)})">
                        <text y="2">{tp}</text>>
                    </g>
                {/each}
            </g>
            {#each $allStudies as d}
                <rect
                        x="{x2(d.cat) + 40}"
                        y="{y(d.src)}"
                        rx="4"
                        ry="4"
                        opacity="0.8"
                        width="{x2.bandwidth()}"
                        height="{y.bandwidth()}"
                        fill="{colorScale(d.type)}"
                        stroke="{colorScale(d.type)}"
                        stroke-width="2px"
                        on:mouseover={(e) => {
                            isHovered = true
                            tooltip_x = e.pageX - 5;
                            tooltip_y = e.pageY - 5;
                            tooltip_ssr_id = d.ssr_id;
                            tooltip_acc_nr = d.acc_nr;
                            tooltip_type = d.type;
                            tooltip_src = d.src;
                            tooltip_date = d.date;
                            tooltip_modality = d.Modality
                            tooltip_pid = d.PatientID
                            tooltip_from = d.from
                            d3.select(e.currentTarget).attr("stroke", "black")
                        }}
                        on:mousemove={(e) => {
                            tooltip_x = e.pageX - 5;
                            tooltip_y = e.pageY - 5;
                        }}
                        on:mouseleave={(e) => {
                            isHovered = false
                            d3.select(e.currentTarget).attr("stroke", colorScale(d.type))
                        }}
                >
                </rect>
            {/each}
            <g>
                {#each img_types as img_type, i}
                    <rect
                            x="{(i + 40) + (i * 110)}"
                            y="{height + margin.top + 10}"
                            width="10px"
                            height="10px"
                            fill="{colorScale(img_type)}"
                    ></rect>
                    <text
                            x="{(i+55) + (i * 110)}"
                            y="{height + margin.top + 19}"
                            font-size="10px"
                    >
                        {img_type}
                    </text>
                {/each}
            </g>
        </svg>
    </div>
{/if}

{#if isHovered}
    <div style="top: {tooltip_y}px; left: {tooltip_x}px;" class="tooltip">
        <table class="tg">
            <tbody>
            <tr>
                <td class="tg-ps66">Source</td>
                <td class="tg-ps66">{tooltip_src}</td>
            </tr>
            <tr>
                <td class="tg-ps66">Type</td>
                <td class="tg-ps66">{tooltip_type}</td>
            </tr>
            <tr>
                <td class="tg-ps66">{tooltip_from === "all_studies" ? "Time (Study)" : "Time (First Image)"}</td>
                <td class="tg-ps66">{tooltip_date}</td>
            </tr>
            <tr>
                <td class="tg-ps66">SSR_ID</td>
                <td class="tg-ps66">{tooltip_ssr_id}</td>
            </tr>
            <tr>
                <td class="tg-ps66">AccessionNumber</td>
                <td class="tg-ps66" style="font-weight: bold">{tooltip_acc_nr}</td>
            </tr>
            <tr>
                <td class="tg-ps66">Modality</td>
                <td class="tg-ps66" style="font-weight: bold">{tooltip_modality}</td>
            </tr>
            <tr>
                <td class="tg-ps66">PatientID</td>
                <td class="tg-ps66" style="font-weight: bold">{tooltip_pid}</td>
            </tr>
            </tbody>
        </table>
    </div>
{/if}

<style>
    .tooltip {
        border: 1px solid #ddd;
        box-shadow: 1px 1px 1px #ddd;
        background: white;
        border-radius: 4px;
        padding: 2px;
        position: absolute;
    }
    .tg  {border:none;border-collapse:collapse;border-color:#ccc;border-spacing:0;}
    .tg td{background-color:#fff;border-color:#ccc;border-style:solid;border-width:0px;color:#333;
        font-family:Arial, sans-serif;font-size:14px;overflow:hidden;padding:1px 1px;word-break:normal;}
    .tg th{background-color:#009870;border-color:#ccc;border-style:solid;border-width:0px;color:#333;
        font-family:Arial, sans-serif;font-size:14px;font-weight:normal;overflow:hidden;padding:1px 1px;word-break:normal;}
    .tg .tg-ps66{font-size:11px;text-align:left;vertical-align:top}
    .tg .tg-0lax{text-align:left;vertical-align:top}
</style>
