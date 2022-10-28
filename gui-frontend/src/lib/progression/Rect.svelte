<svelte:options accessors/>

<script lang="ts">
    import * as d3 from 'd3';
    import { allStudies, testSelection, calcData, getAllStudies, getStudyInfo } from "./store";

    /**
     * Rect component for the HeatMap. Handles position, color, and click events.
     */

    export let size = 30;
    export let x = 5;
    export let y = 5;
    export let open = false;
    export let clicked;
    export let color;
    export let ind_colors;
    export let data;
    export let tooltip_x;
    export let tooltip_y;
    export let tooltip_title;
    export let isHovered;

    let spacing = 0.7;
    let directions = {
            0: {"x": -spacing, "y": -spacing},
            1: {"x": spacing, "y": -spacing},
            2: {"x": -spacing, "y": spacing},
            3: {"x": spacing, "y": spacing}
    }

    let subSquares;

    $: subSquares = [
        {"id": 0, "x": x, "y": y, "color": ind_colors[0]},
        {"id": 1, "x": x + (size / 2), "y": y, "color": ind_colors[1]},
        {"id": 2, "x": x, "y": y + (size / 2), "color": ind_colors[2]},
        {"id": 3, "x": x + (size / 2), "y": y + (size / 2),
            "color": ind_colors[3]}
    ]

</script>

<g>
    {#if open}
        {#each subSquares as s, i}
            <rect
                x="{s.x}"
                y="{s.y}"
                width="{size/2}"
                height="{size/2}"
                rx="2"
                fill="{s.color}"
                transform="translate({directions[i].x}, {directions[i].y})"
                on:click={(e) => {
                    open = false
                }}
            >
            </rect>  
        {/each}
    {:else}
        <rect
            x="{x}"
            y="{y}"
            width="{size}"
            height="{size}"
            rx="2"
            fill="{color}"
            on:mouseover={(e) => {
                isHovered = true
                tooltip_x = e.pageX + 5;
                tooltip_y = e.pageY + 5;
                tooltip_title = data._SSRID
                d3.select(e.target).attr("stroke", "black")
                d3.select(e.target).attr("stroke-width", "2")
            }}
            on:mousemove={(e) => {
                tooltip_x = e.pageX + 5;
                tooltip_y = e.pageY + 5;
            }}
            on:mouseleave={(e) => {
                isHovered = false
                d3.select(e.target).attr("stroke", null)
                d3.select(e.target).attr("stroke-width", null)
            }}
            on:click={async (e) => {
                open = true
                clicked.x = x
                clicked.y = y
                let d = getStudyInfo(calcData(data ?? {}))
                $allStudies = await getAllStudies(data._SSRID)
                testSelection.update(() => d)
            }}>
        </rect>
    {/if}   
</g>