<script>
    import Rect from "./Rect.svelte"
    import * as d3 from "d3";
    import Button from '@smui/button';
    import { rects } from "./store.js";

    /**
     * Entrypoint of the HeatMap module. Composed of Rect modules, which depict the actual information.
     */

    function calcPosition(
        col, row, margin, squareSize,
        xSpace, ySpace
    ) {
        let x = margin.left + (col * squareSize) + (col * xSpace)
        let y = row === 0 ? margin.top :  margin.top + (row * squareSize) + (row * ySpace)
        return [x, y]
    }

    function assignPosition(arr) {
        let arr_copy = Array.from(arr)
        let maxRows = Math.ceil(numSquares / maxCols);
        let idx = 0;
        for (let row of [...Array(maxRows).keys()]) {
            for (let col of [...Array(maxCols).keys()]) {
                let m = {"top": 0, "left": 0}
                let [x, y] = calcPosition(
                    col, row, m, squareSize,
                    xSpace, ySpace
                )
                if (idx < numSquares) {
                    arr_copy[idx].x = x
                    arr_copy[idx].y = y
                }
                idx += 1
            }
        }
        return arr_copy
    }

    function sort(arr, order) {
        let arr_copy = Array.from(arr)
        for (let c of children) {
            c["open"] = false
        }
        switch (order) {
            case "ascending":
                arr_copy.sort((a, b) => d3.ascending(a.color, b.color))
                return assignPosition(arr_copy)
            case "descending":
                arr_copy.sort((a, b) => d3.descending(a.color, b.color))
                return assignPosition(arr_copy)
            default:
                return assignPosition(arr_copy)
        }
    }

    export let d2;
    export let numSquares;
    export let maxCols;
    export let squareSize;

    let xSpace = 2;
    let ySpace = 2;
    let children = [];
    let clicked = {}
    let d2_copy;
    let tooltip_x = "";
    let tooltip_y = "";
    let tooltip_title = "";
    let isHovered = false;

    let sc = d3.scaleLinear()
        .domain([-1, 0, 1, 2])
        .range(["purple", "lightgray", "#677078", "#009870"])

    $: {
        for (let c of children) {
            if (c.x === clicked.x && c.y === clicked.y) {

            } else {
                c["open"] = false
            }
        }
    }

    $: d2_copy = sort(d2)
    $rects = sort(d2)

</script>

<div style="margin-top: 20px">
    <Button on:click={() => {
        d2_copy = sort(d2, "ascending")
    }}>
        Ascending
    </Button>
    <Button on:click={() => {
        d2_copy = sort(d2, "descending")
    }}>
        Descending
    </Button>
    <Button on:click={() => {
        d2_copy = sort(d2)
    }}>
        Reset
    </Button>
</div>

{#if isHovered}
    <div style="top: {tooltip_y}px; left: {tooltip_x}px;" class="tooltip">{tooltip_title}</div>
{/if}

<svg height="{(squareSize + ySpace + 1) * Math.ceil(numSquares / maxCols)}"
     width="{(squareSize + xSpace + 1) * maxCols}">
    {#each d2_copy as d, i}
        <Rect
            x={d.x + 25}
            y={d.y + 25}
            size={squareSize}
            color="{sc(d.color)}"
            ind_colors="{d.ind_color.map(c => sc(c))}"
            data="{d.data}"
            bind:this={children[i]}
            bind:clicked={clicked}
            bind:tooltip_x={tooltip_x}
            bind:tooltip_y={tooltip_y}
            bind:tooltip_title={tooltip_title}
            bind:isHovered={isHovered}
        />
    {/each}
</svg>

<style>
    .tooltip {
        border: 1px solid #ddd;
        box-shadow: 1px 1px 1px #ddd;
        background: white;
        border-radius: 4px;
        padding: 2px;
        position: absolute;
    }
</style>