<svelte:head>
  <link rel="stylesheet" href="node_modules/svelte-material-ui/bare.css" />
  <link rel="stylesheet" href="node_modules/svelte-material-ui/icons.css"/>
</svelte:head>

<script lang="ts">
    import HeatMap from "./lib/progression/HeatMap.svelte"
    import Times from "./lib/progression/Times.svelte";
    import Filter from "./lib/filter/Filter.svelte"
    import {onMount} from "svelte";
    import data from "./lib/data.js"
    import LayoutGrid, { Cell } from "@smui/layout-grid"
    import Drawer, { AppContent, Content, Header, } from '@smui/drawer';
    import List, { Item, Text, Graphic, Separator, Subheader } from '@smui/list';
    import logo from './assets/logo_SRCB.png'
    import { H6 } from "@smui/common/elements"

    /**
     * Entrypoint of the frontend application. Allows to access the Filter or HeatMap (and Times) modules. Note
     * that the HeatMap and Times modules communicate via progression/store.js.
     */

    let open = false;
    let active = 'Patient Filter';
  
    function setActive(value) {
      active = value;
      open = false;
    }

    let appInitialized;
    let d2;

    onMount(async () => {
        try {
          await data.init()
          appInitialized = true;
          d2 = $data;          
        }catch(error) {
          console.error(error)
        }
    })

    let maxCols = 70;
    let squareSize = 16;

    let backend_ip = import.meta.env.VITE_BACKEND_URL
    let mongo_express_port = import.meta.env.VITE_MONGO_EXPRESS_PORT
    let db_name = import.meta.env.VITE_MONGODB_DATABASE_NAME
    let airflow_webserver_port = import.meta.env.VITE_AIRFLOW_WEBSERVER_PORT

  </script>
  
  <div class="drawer-container">
    <!-- Don't include fixed={false} if this is a page wide drawer.
          It adds a style for absolute positioning. -->
    <Drawer style="height: 100vh">
      <Header>
        <img src="{logo}"  alt="logo" width="100%">
      </Header>
      <Content>
        <List>
          <Item
          href="javascript:void(0)"
          on:click={() => setActive('Start')}
          activated={active === 'Start'}
        >
          <Graphic class="material-icons" aria-hidden="true">home</Graphic>
          <Text>Start</Text>
        </Item>
            <Item
            href="javascript:void(0)"
            on:click={() => setActive('Patient Filter')}
            activated={active === 'Patient Filter'}
          >
            <Graphic class="material-icons" aria-hidden="true">filter_alt</Graphic>
            <Text>Search</Text>
          </Item>
          <Item
            href="javascript:void(0)"
            on:click={() => setActive('Progression Map')}
            activated={active === 'Progression Map'}
          >
            <Graphic class="material-icons" aria-hidden="true">cloud_sync</Graphic>
            <Text>Progression</Text>
          </Item>
          <Separator />
          <Subheader component={H6}>Admin</Subheader>
          <Item
            href="http://{backend_ip}:{mongo_express_port}/db/{db_name}/"
            target="_blank"
            on:click={() => setActive('MongoDB')}
            activated={active === 'MongoDB'}
          >
            <Graphic class="material-icons" aria-hidden="true">launch</Graphic>
            <Text>Database</Text>
          </Item>
          <Item
            href="http://{backend_ip}:{airflow_webserver_port}/"
            target="_blank"
            on:click={() => setActive('Airflow')}
            activated={active === 'Airflow'}
          >
            <Graphic class="material-icons" aria-hidden="true">launch</Graphic>
            <Text>Transfer</Text>
          </Item>
        </List>
      </Content>
    </Drawer>

    <!-- Don't include fixed={false} if this is a page wide drawer.
          It adds a style for absolute positioning. -->
    <!-- <Scrim fixed={false} /> -->
    <AppContent class="app-content" style="width: 100%;">
      <main class="main-content">
        {#if active === "Progression Map" && appInitialized}
            <LayoutGrid>
                <Cell span={12}>
                  <HeatMap d2="{d2}" maxCols="{maxCols}" squareSize="{squareSize}" numSquares="{d2.length}"/>
                  <Times />
                </Cell>
            </LayoutGrid>
        {:else if active === "Patient Filter"}
          <Filter />
        {:else}

        {/if}
      </main>
    </AppContent>
  </div>
  
  <style>
    /* These classes are only needed because the
      drawer is in a container on the page. */
    .drawer-container {
      position: relative;
      display: flex;
      height: 100%;
      max-width: 100%;
      border: 1px solid
        var(--mdc-theme-text-hint-on-background, rgba(0, 0, 0, 0.1));
      overflow: hidden;
      z-index: 0;
    }
  </style>
  