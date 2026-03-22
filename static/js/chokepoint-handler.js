/**
 * Chokepoint Handler
 * Renders chokepoint markers and popups on the map.
 * Depends on: map-initialization.js (map, layerVisibility, chokepointMarkers)
 */

window.chokepointMarkers = window.chokepointMarkers || [];

function renderChokepointMarkers(chokepoints) {
    // Clear existing
    if (window.chokepointMarkers) {
        window.chokepointMarkers.forEach(m => { if (map.hasLayer(m)) map.removeLayer(m); });
    }
    window.chokepointMarkers = [];

    if (!chokepoints || chokepoints.length === 0) return;

    chokepoints.forEach(cp => {
        const icon = L.divIcon({
            className: 'chokepoint-icon',
            html: `<div style="background: #ff0000; width: 16px; height: 16px; border-radius: 50%;
                              border: 2px solid #ffffff; box-shadow: 0 0 10px rgba(255, 0, 0, 0.8);"></div>`,
            iconSize:    [16, 16],
            iconAnchor:  [8, 8],
            popupAnchor: [0, -8]
        });

        const marker = L.marker([cp.lat, cp.lon], { icon })
            .bindPopup(buildChokepointPopup(cp), { maxWidth: 320 });

        marker.addTo(map);
        window.chokepointMarkers.push(marker);
    });
}

function buildChokepointPopup(cp) {
    const total = cp.vessel_count_total || 0;

    const vesselTypes = [
        { label: 'Container',     count: cp.vessel_count_container,     color: '#2196F3' },
        { label: 'Dry Bulk',      count: cp.vessel_count_dry_bulk,      color: '#795548' },
        { label: 'General Cargo', count: cp.vessel_count_general_cargo, color: '#4CAF50' },
        { label: 'RoRo',          count: cp.vessel_count_roro,          color: '#9C27B0' },
        { label: 'Tanker',        count: cp.vessel_count_tanker,        color: '#FF9800' },
    ];

    const barRows = vesselTypes.map(v => {
        const pct = total > 0 ? Math.round((v.count / total) * 100) : 0;
        return `
        <div style="margin-bottom:8px;">
            <div style="display:flex; justify-content:space-between; margin-bottom:3px;">
                <span style="font-size:12px; color:#555; font-weight:600;">${v.label}</span>
                <span style="font-size:12px; color:#222; font-weight:700;">${(v.count || 0).toLocaleString()} <span style="color:#aaa; font-weight:400;">(${pct}%)</span></span>
            </div>
            <div style="background:#f0f0f0; border-radius:4px; height:8px; overflow:hidden;">
                <div style="width:${pct}%; background:${v.color}; height:100%; border-radius:4px; transition:width 0.4s;"></div>
            </div>
        </div>`;
    }).join('');

    return `
    <div style="font-family:'Roboto Slab',serif; min-width:270px; max-width:310px;">
        <div style="background:linear-gradient(135deg,#cc0000,#ff0000); padding:13px 16px;
                    margin:-1px -1px 0 -1px; border-radius:6px 6px 0 0;">
            <div style="display:flex; align-items:center; gap:8px;">
                <i class="fas fa-water" style="color:#fff; font-size:18px;"></i>
                <div>
                    <div style="font-size:16px; font-weight:700; color:#fff;">${cp.name}</div>
                    <div style="font-size:12px; color:rgba(255,255,255,0.85); margin-top:2px;">
                        Maritime Chokepoint
                    </div>
                </div>
            </div>
        </div>

        <div style="padding:14px 16px;">
            <!-- Total vessels -->
            <div style="display:flex; align-items:center; justify-content:space-between;
                        padding:10px 12px; background:#fff8e1; border-radius:6px;
                        border-left:4px solid #ff0000; margin-bottom:14px;">
                <span style="font-size:13px; color:#555; font-weight:600;">Total Vessels Tracked</span>
                <span style="font-size:22px; font-weight:800; color:#cc0000;">${total.toLocaleString()}</span>
            </div>

            <!-- Breakdown bars -->
            <div style="font-size:12px; font-weight:700; text-transform:uppercase;
                        letter-spacing:0.5px; color:#888; margin-bottom:10px;">
                Vessel Breakdown
            </div>
            ${barRows}
        </div>
    </div>`;
}

function renderChokepointSidebar(chokepoints) {
    // Inject into route-summary section or a dedicated container if present
    let container = document.getElementById('chokepoint-alerts');
    if (!container) return;

    if (!chokepoints || chokepoints.length === 0) {
        container.innerHTML = '';
        return;
    }

    const items = chokepoints.map(cp => `
        <div style="display:flex; align-items:center; gap:10px; padding:8px 0;
                    border-bottom:1px solid rgba(255,255,255,0.07);">
            <span style="width:28px; height:28px; border-radius:50%;
                         background:linear-gradient(135deg,#e65100,#ff9800);
                         display:flex; align-items:center; justify-content:center; flex-shrink:0;">
                <i class="fas fa-water" style="color:#fff; font-size:13px;"></i>
            </span>
            <div style="flex:1; min-width:0;">
                <div style="font-size:13px; font-weight:700; color:#fff;">${cp.name}</div>
                <div style="font-size:11px; color:#aaa;">${cp.distance_nmi} nmi from route · ${(cp.vessel_count_total || 0).toLocaleString()} vessels</div>
            </div>
        </div>`).join('');

    container.innerHTML = `
        <div style="margin-top:4px;">
            <div style="display:flex; align-items:center; gap:10px; padding:10px 12px;
                        background:rgba(255,0,0,0.12); border-left:4px solid #ff0000;
                        border-radius:6px;">
                <i class="fas fa-water" style="font-size:13px; color:#fff;"></i>
                <div style="font-size:12px; font-weight:700; color:#fff;">
                    ${chokepoints.length} Chokepoint${chokepoints.length > 1 ? 's' : ''} on Route
                </div>
            </div>
        </div>`;
}