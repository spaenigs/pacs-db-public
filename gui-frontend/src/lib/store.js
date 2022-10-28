import { writable } from 'svelte/store';

export const showBanner = writable(false);
export const bannerText = writable("");
