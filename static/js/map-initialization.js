const map = L.map('map', {
    center: [20, 0],
    zoom: 2,
    minZoom: 2,
    maxZoom: 18,
    worldCopyJump: true
});

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors &copy; CartoDB',
    maxZoom: 19,
    keepBuffer: 2,
    updateWhenIdle: true,
    updateWhenZooming: false,
    noWrap: false
}).addTo(map);

const tileLayers = {
    "OpenStreetMap": L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors',
        maxZoom: 19,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    }),
    "OpenSeaMap": L.tileLayer('https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png', {
        attribution: 'Map data: &copy; OpenSeaMap contributors',
        maxZoom: 18,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    }),
    "Satellite": L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Esri, Maxar, Earthstar Geographics',
        maxZoom: 19,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    }),
    "CartoDB Dark": L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap contributors &copy; CartoDB',
        maxZoom: 20,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    }),
    "CartoDB Voyager": L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap contributors &copy; CartoDB',
        maxZoom: 20,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    }),
    "Ocean Depth": L.tileLayer('https://tiles.arcgis.com/tiles/C8EMgrsFcRFL6LrL/arcgis/rest/services/GEBCO_basemap_NCEI/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'GEBCO, NOAA NCEI',
        maxZoom: 13,
        keepBuffer: 2,
        updateWhenIdle: true,
        updateWhenZooming: false,
        noWrap: false
    })
};

L.control.layers(tileLayers, null, { position: 'topright' }).addTo(map);
tileLayers["OpenStreetMap"].addTo(map);

map.options.minZoom = 2;
map.options.maxZoom = 18;

let routeLayers = [];
let portMarkers = [];

function clearMap() {
    window.routeLayers.forEach(l => map.removeLayer(l));
    window.portMarkers.forEach(m => map.removeLayer(m));
    window.routeLayers = [];
    window.portMarkers = [];
}

document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.querySelector(".sidebar");
    const closeBtn = document.getElementById("sidebar-close");
    const openBtn  = document.getElementById("sidebar-open");

    if (closeBtn && openBtn && sidebar) {
        closeBtn.addEventListener("click", () => {
            sidebar.classList.add("collapsed");
            openBtn.style.display = 'block';
            map.invalidateSize();
        });
        openBtn.addEventListener("click", () => {
            sidebar.classList.remove("collapsed");
            openBtn.style.display = 'none';
            map.invalidateSize();
        });
    }

    const legendToggleBtn = document.getElementById("legend-toggle-btn");
    const legend = document.querySelector(".legend");
    if (legendToggleBtn && legend) {
        legendToggleBtn.addEventListener("click", () => {
            const isVisible = legend.style.display !== "none";
            legend.style.display = isVisible ? "none" : "block";
            legendToggleBtn.title = isVisible ? "Show Legend" : "Hide Legend";
        });
    }
});

window.map = map;
window.routeLayers = routeLayers;
window.portMarkers = portMarkers;
window.clearMap = clearMap;