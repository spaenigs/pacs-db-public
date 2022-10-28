<script lang="ts">
    import Tab, { Label } from '@smui/tab';
    import TabBar from '@smui/tab-bar';
    import IconButton from '@smui/icon-button';
    import { tabStore, activeIdx } from "./store"
    import TabN from "./TabN.svelte";

    /**
     * Entrypoint of the patient filter module. Parent component of 1 to N TabN modules.
     */

    function tabStoreEntry(headerName: string, idx: number) {
        return {
            header: headerName, idx: idx, collections: ["Studies"], 
            selectedFields: [
                "_AcquisitionState"
            ], 
            selectedOps: ["equals"], 
            selectedVals: [
                "Internal"
            ], 
            numberOfRows: 1, 
            logical_op: "and", queryRes: []
        }
    }

    function addTab(tabs) {
        let add = tabs.pop()
        let tmpTab = tabStoreEntry("Tab" + (tabs.length + 1), tabs.length) 
        tmpTab.collections = ["Series"]
        tmpTab.selectedFields = ["_SeriesTimeExact"]
        tmpTab.selectedVals = ["2020"]
        tmpTab.selectedOps = ["greater than or equals"]
        tabs.push(tmpTab)
        add.idx = tabs.length
        tabs.push(add)
        return tabs
    }

    $tabStore = [
        tabStoreEntry("Tab1", 0),
        tabStoreEntry("add", 1)
    ]

    let showJoinPage = false

</script>

<TabBar tabs={$tabStore} let:tab>
    {#if tab.header === "add"}
        <IconButton
                class="material-icons"
                title="New Tab"
                on:click={() => {
                    $tabStore = addTab($tabStore)
                    $activeIdx = $tabStore[$tabStore.length - 2].idx
                    showJoinPage = false
                }}
        >add_box</IconButton>
    {:else }
        <Tab {tab} on:click={() => {
            $activeIdx = tab.idx
            showJoinPage = false
        }}>
            <Label>{tab.header}</Label>
        </Tab>
    {/if}
</TabBar>

<div style="margin-top: 20px">
    {#each $tabStore as t, i}
        {#if $activeIdx === i}
            <TabN />
        {/if}
    {/each}
</div>


